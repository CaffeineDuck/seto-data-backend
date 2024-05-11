from dagster import asset


@asset()
def do_something_man():
    print("do something man")
