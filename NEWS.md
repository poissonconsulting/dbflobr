# dbflobr 0.2.0

- Add replace = FALSE to save_flobs() and save_all_flobs()
- import_flobs() and import_all_flobs() now check directory exists.
- Added sub = FALSE argument to import_all_flobs().
- Added pattern = ".*" argument to import_all_flobs().
- Add pattern argument to import_flobs() to match to file names.
- Require that filenames be unique when importing.
- Added sub = FALSE to import_flobs()
- Added sub argument to save_flobs() and save_all_flobs().
- Added geometry = FALSE argument to save_all_flobs() to ignore columns named geometry by default.


# dbflobr 0.1.0.9000

- Internal changes only.


# dbflobr 0.0.1

- Initial Release

# dbflobr 0.1.0

- Added save_flobs, save_all_flobs, import_flobs and import_all_flobs functions
- Replaced checkr package with chk to upgrade argument checking and testing
