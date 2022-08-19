<!-- NEWS.md is maintained by https://cynkra.github.io/fledge, do not edit -->

# dbflobr 0.2.2

- Internal changes only.


# dbflobr 0.2.1

- Change maintainer


# dbflobr 0.2.0.9000

- Added `blob = FALSE` argument to `read_flob()` to specify whether to process as blobs instead of flobs.
- Added `blob_ext = NULL` argument to `save_flobs()` to specify the file extension to use if blobs are encountered.

# dbflobr 0.2.0

## New Features

- Added `sub = FALSE` and `sub = FALSE` argument to `import_flobs()`, `import_all_flobs()`, `save_flobs()` and `save_all_flobs()` to import from and save to subdirectories with the name of the primary key.
This feature means rather than renaming files users for import can use the `save` functions to create the subdirectories for the primary key(s) and then drag each file into the correctly named subdirectory and `import`.

- Added `pattern = ".*"` argument to `import_flobs()` and `import_all_flobs()` to match to file names.

- Added `replace = FALSE` argument to `save_flobs()` and `save_all_flobs()` to specify whether to replace existing files.
- Added `geometry = FALSE` argument to `save_all_flobs()` to ignore columns named geometry by default.
- `import_flobs()` and `import_all_flobs()` now check directory exists.

# dbflobr 0.1.0

- Added save_flobs, save_all_flobs, import_flobs and import_all_flobs functions
- Replaced checkr package with chk to upgrade argument checking and testing

# dbflobr 0.0.1

- Initial Release
