from .data_type import DataType

BASE_STRUCTURE = {
    "message_id": DataType.STRING,
    "anonymous_id": DataType.STRING,
    "received_at": DataType.DATETIME,
    "timestamp": DataType.DATETIME,
    "unix_timestamp_in_millis": DataType.INT64,
    "ip": DataType.STRING,
    "channel": DataType.STRING,
    "user_id": DataType.STRING,
    "write_key": DataType.STRING,
    "type": DataType.STRING
}

EVENT_SPECIFIC = {
    "original_event": DataType.STRING,
    "event": DataType.STRING,
}

TRACKS = dict(BASE_STRUCTURE)
TRACKS.update(EVENT_SPECIFIC)
TRACKS_ALLOWED_FIELD_PREFIXES = (
    "context_",
    "traits_",
    "geoip_",
    "e_"
)

IDENTITIES = dict(BASE_STRUCTURE)

PAGES = dict(BASE_STRUCTURE)

SCREENS = dict(BASE_STRUCTURE)

USERS = dict(BASE_STRUCTURE)
USER_SPECIFIC = {"user_id": DataType.STRING, "ver": DataType.INT64}
USERS.update(USER_SPECIFIC)

GROUPS = dict(BASE_STRUCTURE)

ALIASES = dict(BASE_STRUCTURE)

TRACKS_TABLE = "tracks"
SCREENS_TABLE = "screens"
IDENTITIES_TABLE = "identities"
PAGES_TABLE = "pages"
USERS_TABLE = "users"
ALIASES_TABLE = "aliases"
GROUPS_TABLE = "groups"
MISFITS_TABLE = "misfits"

DEFAULT_TABLES = [
    TRACKS_TABLE,
    SCREENS_TABLE,
    IDENTITIES_TABLE,
    PAGES_TABLE,
    USERS_TABLE,
    ALIASES_TABLE,
    GROUPS_TABLE,
    MISFITS_TABLE
]
