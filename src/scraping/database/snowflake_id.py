import time
import threading


class SnowflakeIDGenerator:
    """
    Simplified Snowflake ID generator

    ID structure (64 bits):
    - 1 bit: sign (always 0)
    - 41 bits: timestamp (milliseconds since epoch)
    - 10 bits: machine ID (0-1023)
    - 12 bits: sequence number (0-4095)
    """

    def __init__(self, machine_id: int = 0):
        if not 0 <= machine_id <= 1023:
            raise ValueError("machine_id must be between 0 and 1023")

        self.machine_id = machine_id
        self.sequence = 0
        self.last_timestamp = 0
        self.lock = threading.Lock()

        # Epoch: 2021-01-01 00:00:00 UTC
        self.EPOCH = 1609459200000
        self.MACHINE_ID_BITS = 10
        self.SEQUENCE_BITS = 12

        # Masks
        self.MACHINE_ID_MASK = (1 << self.MACHINE_ID_BITS) - 1
        self.SEQUENCE_MASK = (1 << self.SEQUENCE_BITS) - 1

        # Shifts
        self.MACHINE_ID_SHIFT = self.SEQUENCE_BITS
        self.TIMESTAMP_SHIFT = self.SEQUENCE_BITS + self.MACHINE_ID_BITS

    def _get_timestamp(self) -> int:
        """Get the current timestamp in milliseconds"""
        return int(time.time() * 1000) - self.EPOCH

    def _wait_next_millis(self, last_timestamp: int) -> int:
        """Wait until the next milisecond"""
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._get_timestamp()
        return timestamp

    def generate_id(self) -> int:
        """Generate a new unique Snowflake ID"""
        with self.lock:
            timestamp = self._get_timestamp()

            if timestamp < self.last_timestamp:
                raise RuntimeError("Clock moved backwards. Refusing to generate id")

            if timestamp == self.last_timestamp:
                # Same milisecond, increment sequence
                self.sequence = (self.sequence + 1) & self.SEQUENCE_MASK
                if self.sequence == 0:
                    # Sequence exhausted, wait for next milisecond
                    timestamp = self._wait_next_millis(self.last_timestamp)
            else:
                # new milisecond, reset sequence
                self.sequence = 0

            self.last_timestamp = timestamp

            # Build the ID
            snowflake_id = (
                (timestamp << self.TIMESTAMP_SHIFT)
                | (self.machine_id << self.MACHINE_ID_SHIFT)
                | self.sequence
            )

            return snowflake_id


# Global instance of the generator
_generator = SnowflakeIDGenerator()


def generate_id() -> int:
    return _generator.generate_id()
