from prefect import flow

@flow(log_prints=True)
def hi():
    print("Running")


if __name__ == "__main__":
    hi()