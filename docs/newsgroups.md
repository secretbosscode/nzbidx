# Common Newsgroups

NZBidx expects a list of NNTP groups via the `NNTP_GROUPS` environment variable. While offerings vary by provider, the following hierarchies are common starting points:

- `alt.binaries.movies` – general movie releases
- `alt.binaries.tv` – episodic television
- `alt.binaries.music` – audio and music
- `alt.binaries.games` – PC and console games
- `alt.binaries.pictures.*` – image sets
- `alt.binaries.ebook` – eBooks and magazines

A sample newline-separated list is available in [newsgroups-example.txt](newsgroups-example.txt). Copy or adapt it to populate `NNTP_GROUPS`.

Use `NNTP_IGNORE_GROUPS` to prune groups you do not wish to scan. Supply a comma-separated list of exact names or wildcard patterns.

Check your provider to ensure these groups exist and comply with their usage policies.
