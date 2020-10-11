source venv/bin/activate
export DAGSTER_HOME=$(PWD)/dagster_home
mkdir -p $DAGSTER_HOME
dagit 