from asynkaf import Consumer

def main():
    try:
        consumer = Consumer(bootstrap_servers="localhost:9092", group_id="my-group")
        print("Successfully created Consumer object.")
    except Exception as e:
        print(f"Failed to create Consumer object: {e}")

if __name__ == "__main__":
    main()
