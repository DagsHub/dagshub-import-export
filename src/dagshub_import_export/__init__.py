import requests

def main() -> None:
    print(requests.get("https://google.com").content)
    print("Hello from dagshub-import-export!")

if __name__ == "__main__":
    main()