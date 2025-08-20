# Newsgroups

Utilities for working with NNTP newsgroup lists.

## `group_list.py`

`scripts/group_list.py` validates and normalizes newline-delimited group names.
It removes duplicates, ensures each name matches a basic newsgroup pattern and
can output the result in different formats.

```bash
# Validate and show a comma-separated list
python scripts/group_list.py groups.txt --csv

# Write the normalized list back to a file
NNTP_GROUP_FILE=groups.txt \
python scripts/group_list.py groups.txt --update
```

When `--update` is used, the destination file path is taken from the
`NNTP_GROUP_FILE` environment variable. The input can be read from a file or
`stdin`.
