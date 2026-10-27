"""Constants and policy: what counts as a secret, what is kept for known plugins, exit codes."""
import re

# ---- what is removed from a tool's original_configuration
SECRET_KEY = re.compile(r"token|secret|cred|passw|api_?key|private", re.I)
TREE_KEY = re.compile(r"^tree|picker", re.I)
MAX_STRING_CHARS = 3000

# plugin-name fragment -> configuration keys that are kept (everything else is recorded as omitted)
PLUGIN_WHITELISTS = {
    "big_query_input": {"inputCustomFilter", "customFilterProjectId", "sqlType", "authType"},
    "big_query_output": {"selectedId", "tableWriteMode", "rowInsertBatchSize"},
    "google-sheets": {"workbookID", "worksheetID", "workbookName", "worksheetName", "firstRowContainsNames"},
}

# configuration keys that hold an embedded script (saved to jupyter/ instead of being kept in the node)
SCRIPT_KEYS = ("productionModeScript", "Notebook")

# ---- secret detection for the leak check
MIN_SECRET_LENGTH = 8
HARMLESS_VALUES = {"", "{}", "[]", "true", "false", "none", "null", "0", "1"}
LONG_SECRET_FINGERPRINT = 200          # for secrets > 4000 chars only the first characters are searched for
LONG_SECRET_THRESHOLD = 4000

# ---- python script analysis
CELL_SEPARATOR = re.compile(r"^\s*#{20,}\s*$")
SYNTHETIC_SEPARATOR = "#" * 33
MUTATING_METHODS = {"append", "extend", "insert", "remove", "pop", "update", "add", "discard", "clear", "sort",
                    "reverse", "setdefault", "popitem", "fill", "put", "resize"}

# ---- graph
NO_DATA_NODES = {"BrowseNode", "CommentNode"}

# ---- process exit codes
EXIT_OK = 0
EXIT_SECRET_LEAK = 1
EXIT_CHECK_FAILED = 2
