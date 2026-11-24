"""Exception hierarchy for CareerOS."""


class CareerOSError(Exception):
    """Base class for all CareerOS errors."""


class ConfigError(CareerOSError):
    """Configuration is missing, malformed or violates a safety rule."""


class StorageLocationError(CareerOSError):
    """A data directory was refused (for example, it is inside a git working tree)."""


class DatasetKindMismatchError(CareerOSError):
    """A database holds a different dataset kind (synthetic vs personal) than requested."""


class MigrationError(CareerOSError):
    """A schema migration failed to apply."""


class NotFoundError(CareerOSError):
    """A requested record does not exist."""


class OwnershipLostError(CareerOSError):
    """A write was attempted with a fencing token that is no longer the task's current
    owner (taken over, lease expired, task finished). The write was rolled back."""


class SessionHaltedError(CareerOSError):
    """The session stopped its own workers because it could not renew its leases safely
    (heartbeat failure). Work is left for recovery; nothing is lost."""


class SessionTerminatedError(CareerOSError):
    """The execution session is ending abruptly (e.g. the process is being killed).

    Never caught and converted into a task failure: state is left exactly as a crash
    would leave it, and the next session recovers it.
    """
