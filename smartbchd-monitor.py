#!/usr/bin/env python3
# smartbchd-monitor.py
#
# An exporter for Prometheus and SmartBCH.
#

import json
import logging
import time
import os
import signal
import sys
import socket

from datetime import datetime
from functools import lru_cache
from typing import Any
from typing import Dict
from typing import List
from typing import Union
from wsgiref.simple_server import make_server

import riprova

from bitcoin.rpc import JSONRPCError, InWarmupError, Proxy
from prometheus_client import make_wsgi_app, Gauge, Counter


logger = logging.getLogger("smartbch-exporter")


# Create Prometheus metrics to track smartbchd stats.
SMARTBCH_BLOCK = Gauge("smartbch_block", "Block Height")
SMARTBCH_BLOCK_TRANSACTIONS = Gauge("smartbch_block_transactions", "Transaction in block")
SMARTBCH_BLOCK_VALUES = Gauge("smartbch_block_value", "Total BCH in block")
SMARTBCH_BLOCK_GAS_USED = Gauge("smartbch_block_gas_used", "Gas used in block")
SMARTBCH_BLOCK_GAS_LIMIT = Gauge("smartbch_block_gas_limit", "Gas limit in block")
SMARTBCH_BLOCK_NONCE = Gauge("smartbch_block_nonce", "Block nonce")
SMARTBCH_BLOCK_DIFFICULTY = Gauge("smartbch_block_difficulty", "Block difficulty")
SMARTBCH_BLOCK_UNCLES = Gauge("smartbch_block_uncles", "Block uncles")
SMARTBCH_BLOCK_SIZE_BYTES = Gauge("smartbch_block_size_bytes", "Block size in bytes")
SMARTBCH_BLOCK_TIMESTAMP = Gauge("smartbch_block_timestamp", "Block timestamp")


SMARTBCH_BLOCK_CONTRACTS_CREATED = Gauge("smartbch_block_contracts_created", "Contracts created in block")
SMARTBCH_BLOCK_CONTRACT_ACTIONS = Gauge("smartbch_block_contract_actions", "Contract actions in block")
SMARTBCH_BLOCK_TOKEN_TRANSFERS = Gauge("smartbch_block_token_transfers", "Token transfers in block")
SMARTBCH_BLOCK_BCH_TRANSFERS = Gauge("smartbch_block_bch_transfers", "BCH transfers in block")
SMARTBCH_BLOCK_LOCKED_BCH = Gauge("smartbch_block_locked_bch", "Locked BCH in block")

SMARTBCH_GAS_PRICE = Gauge("smartbch_gas_price", "Gas price")
SMARTBCH_PROTOCOL_VERSION = Gauge("smartbch_protocol_version", "Protocol version")
SMARTBCH_CHAIN_ID = Gauge("smartbch_chain_id", "Chain id")

SMARTBCH_TOTAL_CONTRACTS_CREATED = Gauge("smartbch_total_contracts_created", "Contracts created in total")
SMARTBCH_TOTAL_CONTRACT_ACTIONS = Gauge("smartbch_total_contract_actions", "Contract actions in total")
SMARTBCH_TOTAL_TOKEN_TRANSFERS = Gauge("smartbch_total_token_transfers", "Token transfers in total")
SMARTBCH_TOTAL_BCH_TRANSFERS = Gauge("smartbch_total_bch_transfers", "BCH transfers in total")
SMARTBCH_TOTAL_LOCKED_BCH = Gauge("smartbch_total_locked_bch", "Locked BCH in total")
SMARTBCH_TOTAL_BLACKHOLE_BCH = Gauge("smartbch_total_blackhole_bch", "BCH Fees Burnt in total")


EXPORTER_ERRORS = Counter(
    "smartbch_exporter_errors", "Number of errors encountered by the exporter", labelnames=["type"]
)
PROCESS_TIME = Counter(
    "smartbch_exporter_process_time", "Time spent processing metrics from bitcoin node"
)

SATS_PER_COIN = 1e8
WEI_PER_COIN = SATS_PER_COIN * 1e10

SMARTBCH_RPC_SCHEME = os.environ.get("SMARTBCH_RPC_SCHEME", "http")
SMARTBCH_RPC_HOST = os.environ.get("SMARTBCH_RPC_HOST", "localhost")
SMARTBCH_RPC_PORT = os.environ.get("SMARTBCH_RPC_PORT", "8332")
SMARTBCH_CONF_PATH = os.environ.get("SMARTBCH_CONF_PATH")
METRICS_ADDR = os.environ.get("METRICS_ADDR", "")  # empty = any address
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9332"))
RETRIES = int(os.environ.get("RETRIES", 5))
TIMEOUT = int(os.environ.get("TIMEOUT", 30))
RATE_LIMIT_SECONDS = int(os.environ.get("RATE_LIMIT", 5))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")


RETRY_EXCEPTIONS = (InWarmupError, ConnectionError, socket.timeout)

RpcResult = Union[Dict[str, Any], List[Any], str, int, float, bool, None]


def on_retry(err: Exception, next_try: float) -> None:
    err_type = type(err)
    exception_name = err_type.__module__ + "." + err_type.__name__
    EXPORTER_ERRORS.labels(**{"type": exception_name}).inc()
    logger.error("Retry after exception %s: %s", exception_name, err)


def error_evaluator(e: Exception) -> bool:
    return isinstance(e, RETRY_EXCEPTIONS)


@lru_cache(maxsize=1)
def rpc_client_factory():
    host = SMARTBCH_RPC_HOST
    if SMARTBCH_RPC_PORT:
        host = "{}:{}".format(host, SMARTBCH_RPC_PORT)
    service_url = "{}://{}".format(SMARTBCH_RPC_SCHEME, host)
    logger.info("Using environment configuration")
    return lambda: Proxy(service_url=service_url, timeout=TIMEOUT)


def rpc_client():
    return rpc_client_factory()()


@riprova.retry(
    timeout=TIMEOUT,
    backoff=riprova.ExponentialBackOff(),
    on_retry=on_retry,
    error_evaluator=error_evaluator,
)
def smartbchrpc(*args) -> RpcResult:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("RPC call: " + " ".join(str(a) for a in args))

    result = rpc_client().call(*args)

    logger.debug("Result:   %s", result)
    return result

BLACKHOLE_CONTRACT_ADDRESS="0x0000000000000000000000626c61636b686f6c65"
BRIDGE_CONTRACT_ADDRESS="0xc172f00ac38c8b2004793f94b33483aa704045bb"
BRIDGE_START_BLOCK = 238790 # first block with real txs seeding with bch

lastBlockStatsRead = BRIDGE_START_BLOCK-1

totalContractsCreated = 0
totalTokenTransfers = 0
totalContractActions = 0
totalBchTransfers = 0
totalBchLocked = 0
def refresh_metrics() -> None:
    global lastBlockStatsRead, totalContractsCreated, totalTokenTransfers, totalContractActions, totalBchTransfers, totalBchLocked
    syncing = smartbchrpc("eth_syncing")
    if syncing == False:
        blockHeight = int(smartbchrpc("eth_blockNumber"), base=16)
    else:
        blockHeight = int(smartbchrpc("eth_syncing")['currentBlock'], base=16)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(blockHeight)

    # used for initial boot to catch up on stats
    lastBlock = None
    while lastBlockStatsRead < blockHeight:
        block = smartbchrpc("eth_getBlockByNumber", hex(lastBlockStatsRead), True)
        for tx in block['transactions']:
            if tx['blockNumber'] != hex(lastBlockStatsRead):
                continue
            if tx['from'] == BRIDGE_CONTRACT_ADDRESS:
                totalBchLocked += int(tx['value'], base=16)
            if tx['to'] == BRIDGE_CONTRACT_ADDRESS:
                totalBchLocked -= int(tx['value'], base=16)
            if tx['to'] == '0x0000000000000000000000000000000000000000':
                totalContractsCreated += 1
            if len(tx['input']) >= 10 and tx['input'][0:10] == '0xa9059cbb':
                totalTokenTransfers += 1
            if int(tx['value'], base=16) > 0:
                totalBchTransfers += 1
            else:
                totalContractActions += 1
        lastBlockStatsRead += 1
        lastBlock = block

    if lastBlock is None:
        lastBlock = smartbchrpc("eth_getBlockByNumber", hex(blockHeight - 1), True)
    logger.debug(lastBlock)

    blackholeBchFees = int(smartbchrpc("eth_getBalance", BLACKHOLE_CONTRACT_ADDRESS, hex(blockHeight - 1)), base=16)


    blockContractsCreated = 0
    blockTokenTransfers = 0
    blockContractActions = 0
    blockBchTransfers = 0
    blockBchLocked = 0
    for tx in lastBlock['transactions']:
        if tx['blockNumber'] != lastBlock['blockNumber']:
            continue
        if tx['from'] == BRIDGE_CONTRACT_ADDRESS:
            blockBchLocked += int(tx['value'], base=16)
        if tx['to'] == BRIDGE_CONTRACT_ADDRESS:
            blockBchLocked -= int(tx['value'], base=16)
        if tx['to'] == '0x0000000000000000000000000000000000000000':
            blockContractsCreated += 1
        if len(tx['input']) >= 10 and tx['input'][0:10] == '0xa9059cbb':
            blockTokenTransfers += 1
        if int(tx['value'], base=16) > 0:
            blockBchTransfers += 1
        else:
            blockContractActions += 1


    SMARTBCH_BLOCK.set(blockHeight)
    SMARTBCH_BLOCK_TRANSACTIONS.set(len(lastBlock['transactions']))
    # SMARTBCH_BLOCK_VALUES = Gauge("smartbch_block_value", "Total BCH in block")
    SMARTBCH_BLOCK_GAS_USED.set(int(lastBlock['gasUsed'], base=16))
    SMARTBCH_BLOCK_GAS_LIMIT.set(int(lastBlock['gasLimit'], base=16))
    SMARTBCH_BLOCK_NONCE.set(int(lastBlock['nonce'], base=16))
    SMARTBCH_BLOCK_DIFFICULTY.set(int(lastBlock['difficulty'], base=16))
    SMARTBCH_BLOCK_UNCLES.set(len(lastBlock['uncles']))
    SMARTBCH_BLOCK_SIZE_BYTES.set(int(lastBlock['size'], base=16))
    SMARTBCH_BLOCK_TIMESTAMP.set(int(lastBlock['timestamp'], base=16))


    SMARTBCH_GAS_PRICE.set(int(smartbchrpc("eth_gasPrice"), base=16))
    SMARTBCH_PROTOCOL_VERSION.set(int(smartbchrpc("eth_protocolVersion"), base=16))
    SMARTBCH_CHAIN_ID.set(int(smartbchrpc("eth_chainId"), base=16))

    SMARTBCH_TOTAL_LOCKED_BCH.set(totalBchLocked / WEI_PER_COIN)
    SMARTBCH_TOTAL_CONTRACTS_CREATED.set(totalContractsCreated)
    SMARTBCH_TOTAL_CONTRACT_ACTIONS.set(totalContractActions)
    SMARTBCH_TOTAL_TOKEN_TRANSFERS.set(totalTokenTransfers)
    SMARTBCH_TOTAL_BCH_TRANSFERS.set(totalBchTransfers)
    SMARTBCH_TOTAL_BLACKHOLE_BCH.set(blackholeBchFees / WEI_PER_COIN)

    SMARTBCH_BLOCK_LOCKED_BCH.set(blockBchLocked / WEI_PER_COIN)
    SMARTBCH_BLOCK_CONTRACTS_CREATED.set(blockContractsCreated)
    SMARTBCH_BLOCK_CONTRACT_ACTIONS.set(blockContractActions)
    SMARTBCH_BLOCK_TOKEN_TRANSFERS.set(blockTokenTransfers)
    SMARTBCH_BLOCK_BCH_TRANSFERS.set(blockBchTransfers)

def sigterm_handler(signal, frame) -> None:
    logger.critical("Received SIGTERM. Exiting.")
    sys.exit(0)


def exception_count(e: Exception) -> None:
    err_type = type(e)
    exception_name = err_type.__module__ + "." + err_type.__name__
    EXPORTER_ERRORS.labels(**{"type": exception_name}).inc()


def main():
    # Set up logging to look similar to bitcoin logs (UTC).
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ"
    )
    logging.Formatter.converter = time.gmtime
    logger.setLevel(LOG_LEVEL)

    # Handle SIGTERM gracefully.
    signal.signal(signal.SIGTERM, sigterm_handler)

    app = make_wsgi_app()

    last_refresh = datetime.fromtimestamp(0)

    def refresh_app(*args, **kwargs):
        nonlocal last_refresh
        process_start = datetime.now()

        # Only refresh every RATE_LIMIT_SECONDS seconds.
        if (process_start - last_refresh).total_seconds() < RATE_LIMIT_SECONDS:
            return app(*args, **kwargs)

        # Allow riprova.MaxRetriesExceeded and unknown exceptions to crash the process.
        try:
            refresh_metrics()
        except riprova.exceptions.RetryError as e:
            logger.error("Refresh failed during retry. Cause: " + str(e))
            exception_count(e)
        except JSONRPCError as e:
            logger.debug("SmartBCH RPC error refresh", exc_info=True)
            exception_count(e)
        except json.decoder.JSONDecodeError as e:
            logger.error("RPC call did not return JSON. Bad credentials? " + str(e))
            sys.exit(1)

        duration = datetime.now() - process_start
        PROCESS_TIME.inc(duration.total_seconds())
        logger.info("Refresh took %s seconds", duration)
        last_refresh = process_start

        return app(*args, **kwargs)

    httpd = make_server(METRICS_ADDR, METRICS_PORT, refresh_app)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
