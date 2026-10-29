"""Application-level network emulator for the PC <-> cloud uplink.

Real `tc netem` needs Linux root; this does the same job inside Python so
the testbed runs on any OS. Every impairment is driven by the scenario's
run config, so all architectures see identical conditions.

    delay_ms        one-way delay added on send and again on receive
    jitter_ms       +/- uniform jitter on each one-way delay
    loss            probability a request is lost (caller sees a timeout)
    bandwidth_kbps  serialisation cap; requests queue behind each other
    outages         [[start_s, end_s], ...] cloud unreachable (relative to t0)
"""
import asyncio
import random

LOSS_DETECT_S = 1.0  # how long the sender waits before declaring a lost request


class LinkError(Exception):
    """The emulated link refused, lost or timed out a request."""


class NetEm:
    def __init__(self, cfg: dict, t0: float, seed: int = 0, base_delay_ms: float = 0.0,
                 queue_timeout_s: float = 5.0):
        cfg = cfg or {}
        self.delay_ms = float(cfg.get("delay_ms", 0)) + base_delay_ms
        self.jitter_ms = float(cfg.get("jitter_ms", 0))
        self.loss = float(cfg.get("loss", 0))
        self.bandwidth_kbps = float(cfg.get("bandwidth_kbps", 0))
        self.outages = [tuple(w) for w in cfg.get("outages", [])]
        self.t0 = t0
        self.queue_timeout_s = queue_timeout_s
        self._rng = random.Random(seed)
        self._link_free_at = 0.0

    def in_outage(self, t: float) -> bool:
        rel = t - self.t0
        return any(start <= rel < end for start, end in self.outages)

    def _one_way(self) -> float:
        if self.delay_ms <= 0 and self.jitter_ms <= 0:
            return 0.0
        d = self.delay_ms + self._rng.uniform(-self.jitter_ms, self.jitter_ms)
        return max(0.0, d) / 1000.0

    async def before_send(self, nbytes: int, t: float):
        """Apply outage, bandwidth queueing, delay and loss to an outgoing request."""
        if self.in_outage(t):
            raise LinkError("unreachable")
        if self.bandwidth_kbps > 0:
            tx_time = nbytes * 8 / (self.bandwidth_kbps * 1000)
            start = max(t, self._link_free_at)
            if start - t > self.queue_timeout_s:
                raise LinkError("queue-timeout")   # drop-tail: link backlog too long
            self._link_free_at = start + tx_time
            await asyncio.sleep(self._link_free_at - t)
        delay = self._one_way()
        if delay:
            await asyncio.sleep(delay)
        if self.loss and self._rng.random() < self.loss:
            await asyncio.sleep(LOSS_DETECT_S)
            raise LinkError("lost")

    async def after_receive(self):
        delay = self._one_way()
        if delay:
            await asyncio.sleep(delay)
