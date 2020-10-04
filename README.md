# Snowflake_CC_Dmo
This repository will help you to get started
## Getting Started

### 1. Installing snowsql (CLI) Windows
After installing of the CLI, change the example connection inside of the config file (create a backup first).

```$
vim %USERPROFILE%\.snowsql\config
```

```$
[connections]

[connections.example]
accountname = "<account_name>.us-east-1"
username = "<user_name>"
password = "<pass_word>"
dbname = "<default_database>"
warehousename = "<defualt_vwh>"
rolename = "<default_<role>"
schemaname = "<default_schema>"

[connections.example2]
accountname = "compucom.us-east-1"
...

[variables]
#Loads these variables on startup
#Can be used in SnowSql as select $example_variable
example_variable="Hi Luis"
example_variable2=1

[options]
auto_completion = True
friendly = True
rowset_size = 1000
variable_substitution = True

log_level = "ERROR"
```

### 2. Connecting to Snowflake

```$
// Using the example conecction
snowsql -c example
```

```$
// Using specific version
snowsql -v 1.2.9 -c example
```

```$
// Using specific username & password
snowsql -a compucom.us-east-1 -u lfuentes
```
### 3. Advance features 
```$
snowsql -c example -D IPCC_SK=5369754302 -f C:\Users\lf188653\Desktop\CC\SnowflakeCloud\TestingBinding.sql -o output_file=C:\Users\lf188653\Desktop\CC\SnowflakeCloud\output.csv -o quiet=true -o friendly=false -o header=True -o output_format=csv
```
In a Snowflake session, you can issue commands to take specific actions. All commands in SnowSQL start with an exclamation point (!), followed by the command name.

```$
!help
```

To run a scrip file in snowsql:

```$
!source C:\~\Testing.sql
```
```$
snowsql -f C:\~\Testing.sql
```

To clean scren of commands:
ctrl+l


## Authors

* **Luis Enrique Fuentes Plata ** - *2020/10/04*