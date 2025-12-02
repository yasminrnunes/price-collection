"""Snowflake ID generator for unique identifier generation.

This module provides a simplified implementation of Twitter's Snowflake ID
algorithm for generating unique, sortable, distributed IDs. The IDs are 64-bit
integers that can be generated across multiple machines without coordination.

The Snowflake ID structure (64 bits):
    - 1 bit:  Sign bit (always 0, ensuring positive integers)
    - 41 bits: Timestamp (milliseconds since custom epoch)
    - 10 bits: Machine ID (0-1023, allows up to 1024 machines)
    - 12 bits: Sequence number (0-4095, allows 4096 IDs per millisecond)

The module provides:
    - Thread-safe ID generation using locks
    - Clock skew detection and error handling
    - Automatic sequence management within milliseconds
    - Global generator instance for convenience

Characteristics:
    - Unique: Each ID is guaranteed to be unique within the system
    - Sortable: IDs are roughly chronological due to timestamp component
    - Distributed: Multiple machines can generate IDs independently
    - High throughput: Can generate up to 4,096 IDs per millisecond per machine

Example:
    Use the global function for simple ID generation:
        >>> from database.snowflake_id import generate_id
        >>> id1 = generate_id()
        >>> id2 = generate_id()
        >>> assert id1 != id2

    Or create a custom generator with specific machine ID:
        >>> generator = SnowflakeIDGenerator(machine_id=42)
        >>> custom_id = generator.generate_id()

Note:
    The epoch is set to 2021-01-01 00:00:00 UTC, providing approximately
    69 years of unique ID generation before timestamp overflow.
"""

import time
import threading


class SnowflakeIDGenerator:
    """Simplified Snowflake ID generator for unique identifier generation.

    This class implements a thread-safe Snowflake ID generator that creates
    unique, sortable 64-bit integer IDs. The IDs are composed of timestamp,
    machine ID, and sequence number components, allowing for high-throughput
    ID generation across distributed systems.

    ID Structure (64 bits):
        - 1 bit:  Sign bit (always 0, ensures positive integers)
        - 41 bits: Timestamp in milliseconds since custom epoch (2021-01-01)
        - 10 bits: Machine ID (allows up to 1024 different machines)
        - 12 bits: Sequence number (allows 4096 IDs per millisecond per machine)

    The generator is thread-safe and handles clock skew by raising an error
    if the system clock moves backwards. When the sequence is exhausted within
    a millisecond, it automatically waits for the next millisecond.

    Attributes:
        machine_id: The unique identifier for this machine (0-1023).
        sequence: Current sequence number within the millisecond (0-4095).
        last_timestamp: Timestamp of the last generated ID (for sequence management).
        lock: Thread lock ensuring thread-safe ID generation.
        EPOCH: Custom epoch timestamp in milliseconds (2021-01-01 00:00:00 UTC).

    Example:
        Create a generator with a specific machine ID:
            >>> generator = SnowflakeIDGenerator(machine_id=42)
            >>> id1 = generator.generate_id()
            >>> id2 = generator.generate_id()
            >>> assert id1 < id2  # IDs are roughly chronological
    """

    def __init__(self, machine_id: int = 0):
        """Initialize a Snowflake ID generator instance.

        Args:
            machine_id: Unique identifier for this machine/process. Must be
                between 0 and 1023 inclusive. Each machine in a distributed
                system should have a unique machine_id to avoid collisions.

        Raises:
            ValueError: If machine_id is not within the valid range (0-1023).
        """
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
        """Get the current timestamp in milliseconds relative to the epoch.

        Returns:
            Current timestamp in milliseconds, adjusted by the custom epoch
            (2021-01-01 00:00:00 UTC). This provides approximately 69 years
            of unique ID generation capacity.
        """
        return int(time.time() * 1000) - self.EPOCH

    def _wait_next_millis(self, last_timestamp: int) -> int:
        """Wait until the next millisecond is available.

        This method is called when the sequence number is exhausted within
        the current millisecond. It busy-waits until the next millisecond
        to ensure unique ID generation.

        Args:
            last_timestamp: The timestamp of the last generated ID.

        Returns:
            A timestamp value that is greater than last_timestamp, representing
            the next available millisecond.
        """
        timestamp = self._get_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._get_timestamp()
        return timestamp

    def generate_id(self) -> int:
        """Generate a new unique Snowflake ID.

        This method generates a thread-safe, unique 64-bit integer ID by
        combining the current timestamp, machine ID, and sequence number.
        The method handles sequence overflow within milliseconds and detects
        clock skew issues.

        Returns:
            A unique 64-bit integer Snowflake ID. The ID is roughly chronological
            due to the timestamp component, making it useful for sorting.

        Raises:
            RuntimeError: If the system clock moves backwards (clock skew detected).
                This is a safety measure to prevent ID collisions.

        Example:
            >>> generator = SnowflakeIDGenerator()
            >>> id1 = generator.generate_id()
            >>> id2 = generator.generate_id()
            >>> assert id1 != id2
            >>> assert id2 > id1  # Generally true due to timestamp ordering
        """
        with self.lock:
            timestamp = self._get_timestamp()

            if timestamp < self.last_timestamp:
                raise RuntimeError("Clock moved backwards. Refusing to generate id")

            if timestamp == self.last_timestamp:
                # Same millisecond, increment sequence
                self.sequence = (self.sequence + 1) & self.SEQUENCE_MASK
                if self.sequence == 0:
                    # Sequence exhausted, wait for next millisecond
                    timestamp = self._wait_next_millis(self.last_timestamp)
            else:
                # New millisecond, reset sequence
                self.sequence = 0

            self.last_timestamp = timestamp

            # Build the ID by combining timestamp, machine_id, and sequence
            snowflake_id = (
                (timestamp << self.TIMESTAMP_SHIFT)
                | (self.machine_id << self.MACHINE_ID_SHIFT)
                | self.sequence
            )

            return snowflake_id


# Global instance of the generator with default machine_id (0)
_generator = SnowflakeIDGenerator()


def generate_id() -> int:
    """Generate a new unique Snowflake ID using the global generator instance.

    This is a convenience function that uses a global SnowflakeIDGenerator
    instance with machine_id=0. For most use cases, this is sufficient. If
    you need multiple machines or processes, create separate SnowflakeIDGenerator
    instances with different machine_id values.

    Returns:
        A unique 64-bit integer Snowflake ID from the global generator.

    Example:
        >>> from database.snowflake_id import generate_id
        >>> product_id = generate_id()
        >>> discount_id = generate_id()
        >>> assert product_id != discount_id

    Note:
        This function is thread-safe and uses the same global generator instance
        across all calls. For distributed systems, consider creating generator
        instances with unique machine_id values per machine/process.
    """
    return _generator.generate_id()
