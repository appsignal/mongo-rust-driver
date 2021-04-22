# 0.13.2
* Fix probe of libbson so static linking actually works

# 0.13.1
* Statically link libmongoc

# 0.13.0
* Bump bson dependency to 1.2
* Upgrade libmongoc to 1.17.4
* Remove Windows support

# 0.12.1
* Add support for commands that return batches (by MiesJansen)

# 0.12.0
* Upgrade libmongoc to 1.8.2
* Update bson dependency to 0.11
* Parse MongoDB server responses with lossy UTF-8 decoding to work
  around https://jira.mongodb.org/browse/SERVER-24007

# 0.11.0
* Update bson dependency to 0.10
* Use installed libmongoc if it right version is present on the system (by Matrix-Zhang)

# 0.10.0
* Initial upgrade to mongo c driver 1.8.0, no support for new features yet

# 0.9.0
* Add WriteConcernError and DuplicateKey to error
* Add error code for unknown error
* Bulk operation result which includes reply
* Don't use natural option in tail anymore

# 0.8.0
* Upgrade bson dependency to 0.9

# 0.7.2
* Always use openssl on MacOS
* Make uri send and sync

# 0.7.1
* Fix docs link

# 0.7.0
* Use symver version requirements for dependencies

# 0.6.0
* Upgrade Mongo C driver to 1.6.3

# 0.5.0
* Upgrade Mongo C driver to 1.5.3

# 0.4.0
* Upgrade Mongo C driver to 1.4.0

# 0.3.0
* Support for aggregations

# 0.2.2
* Publicly export MongoErrorDomain and MongoErrorCode

# 0.2.1
* Add documentation url to Cargo.toml

# 0.2.0
* Refactored API, less namespaces than before.
* Upgrade Mongo C driver to 1.3.1
* Now dual licensed as both Apache and MIT

# 0.1.0
* Initial release
