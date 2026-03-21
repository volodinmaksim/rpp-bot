import json
from collections import deque

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message

from config import settings
from loader import logger, redis

BOT_NAME = "rpp"
EXCHANGE_NAME = "telegram_updates"
MAIN_QUEUE_NAME = f"{BOT_NAME}_updates"
RETRY_QUEUE_NAME = f"{BOT_NAME}_updates_retry"
DEAD_QUEUE_NAME = f"{BOT_NAME}_updates_dead"
MAIN_ROUTING_KEY = f"{BOT_NAME}.update"
RETRY_ROUTING_KEY = f"{BOT_NAME}.retry"
DEAD_ROUTING_KEY = f"{BOT_NAME}.dead"
PROCESSING_TTL_SECONDS = 10 * 60
PROCESSED_TTL_SECONDS = 3 * 24 * 60 * 60
PROCESSED_UPDATES_LIMIT = 5000

_rabbit_connection = None
_rabbit_channel = None
_rabbit_exchange = None
_rabbit_main_queue = None
_processed_update_ids_queue: deque[int] = deque(maxlen=PROCESSED_UPDATES_LIMIT)
_processed_update_ids: set[int] = set()
_processing_update_ids: set[int] = set()


def _require_rabbitmq_url() -> str:
    if not settings.RABBITMQ_URL:
        raise RuntimeError("RABBITMQ_URL is not configured")
    return settings.RABBITMQ_URL


def _processing_update_key(update_id: int) -> str:
    return f"tgbot:processing:{BOT_NAME}:{update_id}"


def _processed_update_key(update_id: int) -> str:
    return f"tgbot:processed:{BOT_NAME}:{update_id}"


def _serialize_message(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


async def init_rabbitmq(set_qos: bool = False) -> None:
    global _rabbit_connection, _rabbit_channel, _rabbit_exchange, _rabbit_main_queue

    if _rabbit_connection is not None:
        return

    _rabbit_connection = await aio_pika.connect_robust(_require_rabbitmq_url())
    _rabbit_channel = await _rabbit_connection.channel()
    if set_qos:
        await _rabbit_channel.set_qos(prefetch_count=settings.RABBITMQ_PREFETCH)

    _rabbit_exchange = await _rabbit_channel.declare_exchange(
        EXCHANGE_NAME,
        ExchangeType.DIRECT,
        durable=True,
    )

    _rabbit_main_queue = await _rabbit_channel.declare_queue(
        MAIN_QUEUE_NAME,
        durable=True,
    )
    retry_queue = await _rabbit_channel.declare_queue(
        RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": EXCHANGE_NAME,
            "x-dead-letter-routing-key": MAIN_ROUTING_KEY,
            "x-message-ttl": settings.RABBITMQ_RETRY_DELAY_MS,
        },
    )
    dead_queue = await _rabbit_channel.declare_queue(
        DEAD_QUEUE_NAME,
        durable=True,
    )

    await _rabbit_main_queue.bind(_rabbit_exchange, routing_key=MAIN_ROUTING_KEY)
    await retry_queue.bind(_rabbit_exchange, routing_key=RETRY_ROUTING_KEY)
    await dead_queue.bind(_rabbit_exchange, routing_key=DEAD_ROUTING_KEY)


async def close_rabbitmq() -> None:
    global _rabbit_connection, _rabbit_channel, _rabbit_exchange, _rabbit_main_queue

    if _rabbit_channel is not None:
        await _rabbit_channel.close()
    if _rabbit_connection is not None:
        await _rabbit_connection.close()

    _rabbit_connection = None
    _rabbit_channel = None
    _rabbit_exchange = None
    _rabbit_main_queue = None


async def publish_update(update_data: dict, attempt: int = 0) -> None:
    await init_rabbitmq()
    message_payload = {
        "bot_name": BOT_NAME,
        "update_id": update_data["update_id"],
        "attempt": attempt,
        "payload": update_data,
    }
    await _rabbit_exchange.publish(
        Message(
            body=_serialize_message(message_payload),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=MAIN_ROUTING_KEY,
    )


async def publish_retry(payload: dict) -> None:
    await init_rabbitmq()
    await _rabbit_exchange.publish(
        Message(
            body=_serialize_message(payload),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=RETRY_ROUTING_KEY,
    )


async def publish_dead(payload: dict, error_message: str) -> None:
    await init_rabbitmq()
    dead_payload = {
        **payload,
        "last_error": error_message[:1000],
    }
    await _rabbit_exchange.publish(
        Message(
            body=_serialize_message(dead_payload),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
        ),
        routing_key=DEAD_ROUTING_KEY,
    )


async def consume_updates(handler) -> None:
    await init_rabbitmq(set_qos=True)
    await _rabbit_main_queue.consume(handler)


async def _remember_processed_update(update_id: int) -> None:
    if redis is not None:
        await redis.set(
            _processed_update_key(update_id),
            "1",
            ex=PROCESSED_TTL_SECONDS,
        )
        return

    if len(_processed_update_ids_queue) == PROCESSED_UPDATES_LIMIT:
        stale_update_id = _processed_update_ids_queue.popleft()
        _processed_update_ids.discard(stale_update_id)

    _processed_update_ids_queue.append(update_id)
    _processed_update_ids.add(update_id)


async def release_processing_update(update_id: int) -> None:
    if redis is not None:
        await redis.delete(_processing_update_key(update_id))
        return

    _processing_update_ids.discard(update_id)


async def try_acquire_update(update_id: int) -> bool:
    if redis is not None:
        if await redis.exists(_processed_update_key(update_id)):
            return False

        acquired = await redis.set(
            _processing_update_key(update_id),
            "1",
            ex=PROCESSING_TTL_SECONDS,
            nx=True,
        )
        return bool(acquired)

    if update_id in _processed_update_ids or update_id in _processing_update_ids:
        return False

    _processing_update_ids.add(update_id)
    return True


async def mark_update_processed(update_id: int) -> None:
    await _remember_processed_update(update_id)
    await release_processing_update(update_id)


async def handle_worker_failure(message: aio_pika.abc.AbstractIncomingMessage, payload: dict, exc: Exception) -> None:
    update_id = payload["update_id"]
    attempt = int(payload.get("attempt", 0))

    await release_processing_update(update_id)

    try:
        if attempt >= settings.RABBITMQ_MAX_RETRIES:
            await publish_dead(payload, str(exc))
            logger.error("Update %s moved to dead-letter queue: %s", update_id, exc)
        else:
            payload["attempt"] = attempt + 1
            await publish_retry(payload)
            logger.warning(
                "Update %s scheduled for retry #%s: %s",
                update_id,
                payload["attempt"],
                exc,
            )
        await message.ack()
    except Exception as publish_exc:
        logger.error(
            "Failed to route update %s after error %s: %s",
            update_id,
            exc,
            publish_exc,
            exc_info=True,
        )
        await message.nack(requeue=True)
