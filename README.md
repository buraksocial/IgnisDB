# 🔥 IgnisDB: Let's Build a Key-Value Database!

[![GitHub stars](https://img.shields.io/github/stars/buraksocial/IgnisDB?style=social)](https://github.com/buraksocial/IgnisDB/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/buraksocial/IgnisDB?style=social)](https://github.com/buraksocial/IgnisDB/network/members)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub issues](https://img.shields.io/github/issues/buraksocial/IgnisDB)](https://github.com/buraksocial/IgnisDB/issues)

> Your hands-on guide to building a database from scratch, using Python and asyncio.

Hey there! Welcome to **IgnisDB**. This isn't just another folder of code; it's a fun, hands-on adventure where we build a high-performance, in-memory Key-Value database completely from scratch, all using Python's awesome `asyncio` library.

Think of it as your personal guided tour into how modern systems *really* work. We'll roll up our sleeves and dive deep into network programming, concurrency, clever data structures, and the secrets behind how databases actually save your data without losing it.

> The main idea here is simple, and it's a powerful one: the best way to truly understand something is to build it yourself!

### So, Why Bother Building a Database?

That's a fantastic question! Let's be real, we're not trying to build the next Redis or create a product to sell. Instead, IgnisDB is all about the joy of learning, tinkering, and getting your hands dirty. It’s for any developer who's ever found themselves wondering:

* **"How does the internet even work?"** Seriously! What's actually happening when a client talks to a server? We'll demystify it by building our own little piece of the internet with TCP sockets. It's like setting up a direct, private phone line between two programs.
* **"What's the magic behind `asyncio`?"** How can a single script handle thousands of connections at once without crashing and burning? We'll explore the power of asynchronous programming. Imagine a super-efficient waiter who can take orders from ten tables at once without getting flustered, instead of hiring ten different waiters for ten tables. That's `asyncio`!
* **"How do databases not lose my data when I turn them off?"** We'll tackle persistence by implementing both "snapshotting" and "Append-Only File (AOF)" features. Think of it like taking a perfect photograph of everything (snapshot) or logging every single action (AOF).
* **"How do you build big, complex things without losing your mind?"** Big projects can be intimidating. We'll see how to break down a complex system into small, manageable pieces that are easy to reason about and even easier to build upon. It’s like building with LEGOs—each piece has a purpose and connects neatly to the others.

Basically, if you're tired of just *using* tools and want to feel the thrill of *building* them, you're in exactly the right place.

### 🏛️ How It Works: A Quick Tour

IgnisDB has a pretty straightforward and clean design, which is great because it means it's easy to understand. When a client sends a command, here's the epic journey it takes through our system:

```text
                  +--------------+      +-----------------+      +----------------+
[Client] <------> |  TCP Server  |----->| Protocol Parser |----->| Storage Engine |
                  | (The Bouncer)|      | (The Translator)|      |  (The Brains)  |
                  +--------------+      +-----------------+      +----------------+
````

1.  **The TCP Server (The Front Door)**: This is our bouncer. It stands at the door, listening for new clients who want to connect. For every new friend that knocks, it says "Welcome\!" and starts a new, dedicated task to listen to their requests, so everyone gets attention without waiting in line.
2.  **The Protocol Parser (The Translator)**: Computers love to talk in bytes, but humans prefer words\! This part is our universal translator. It takes the raw, garbled data from the client and translates it into clean commands and arguments that the rest of our application can easily understand, like `GET my_key`.
3.  **The Storage Engine (The Brains)**: This is the heart and soul of the database. It takes the translated commands and actually does the work—storing data, fetching it, deleting it, and so on. It's also the super-organized librarian who knows exactly where every piece of information is and uses a special `Lock` to make sure that even if two people ask for the same book at once, there's no chaos.
4.  **The Response (Sending it back\!)**: Once the engine has an answer (or a confirmation), the parser formats it back into a response the client can understand and sends it back over the wire. Job done\!

### 🚀 What's Inside? (The Cool Features)

  * **Totally Async & Efficient**: Thanks to `asyncio`, IgnisDB can juggle tons of connections at once without breaking a sweat. It's super efficient because it doesn't wait around.
  * **Flexible In-Memory Storage**: We support Strings, Lists, and Hashes, making it more than just a simple key-value store. Everything is in RAM, which means lightning-fast operations.
  * **Dual-Mode Data Persistence**: Choose your durability strategy\!
      * **Snapshotting**: Periodically save a "snapshot" of all data to a file.
      * **Append-Only File (AOF)**: Log every write command to a file for finer-grained durability.
  * **Atomic Transactions**: Group multiple commands into a single, all-or-nothing operation using `MULTI` and `EXEC`. This guarantees data consistency during complex operations.
  * **Master-Slave Replication**: Scale your reads and improve availability. A master server handles writes and propagates them to one or more read-only slave replicas.
  * **Automatic Key Expiration (TTL)**: Set keys to automatically disappear after a certain amount of time. Super useful for caches and session data.
  * **Guaranteed Concurrency Safe**: We use `asyncio.Lock` to enforce a "one at a time, please\!" rule for write operations, preventing data corruption.
  * **Clean and Tidy Code**: We've intentionally kept the project split into logical parts, making it easy to read, understand, and extend.

### 🛠️ The Tech We're Using

  * **Language**: Python 3.9+
  * **Core Library**: `asyncio`
  * **Testing**: `pytest` and `pytest-asyncio`

### ⚙️ Get It Running\!

1.  **Clone the project:**

    ```bash
    git clone [https://github.com/buraksocial/IgnisDB.git](https://github.com/buraksocial/IgnisDB.git)
    cd IgnisDB
    ```

2.  **Install dependencies (optional, no external libraries needed):**

    ```bash
    # It's always a good idea to use a virtual environment!
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Fire up the server\!**
    You can run the server in different modes.

      * **As a Master (default):**
        ```bash
        python ignisdb_server.py
        ```
      * **As a Slave Replica:**
        (Assuming a master is running on the default port 6380)
        ```bash
        python ignisdb_server.py --role slave --port 6381
        ```

4.  **Talk to it:**
    Open a brand new terminal window and use `telnet` to connect:

    ```bash
    telnet 127.0.0.1 6380
    ```

    Now you have a direct line to the database. You can start sending commands\!

    ```
    # Set a key
    SET name 'John Doe'

    # Get it back
    GET name
    > 'John Doe'

    # Work with lists
    LPUSH mylist item1 item2
    > (integer) 2

    LRANGE mylist 0 -1
    > 1) "item2"
    > 2) "item1"
    ```

### 🗺️ What's Next? (The New Roadmap)

This project is always a work in progress\! We've completed the initial roadmap and now have our sights set on even more advanced features. These are great starting points if you want to contribute\!

  * [ ] **Build a Benchmarking Tool:** Create a separate script to measure the server's performance (ops/sec) for various commands to see how our changes impact speed.
  * [ ] **Add More Data Structures (e.g., Sets):** Implement the Set data structure with commands like `SADD`, `SMEMBERS`, and `SISMEMBER` for handling unique, unordered collections.
  * [ ] **Implement a Pub/Sub System:** Add `PUBLISH` and `SUBSCRIBE` commands to allow for real-time, channel-based messaging patterns, turning IgnisDB into a message broker.
  * [ ] **Add Security with Authentication:** Introduce an `AUTH <password>` command to require clients to authenticate before executing other commands.
  * [ ] **Implement AOF Compaction/Rewrite:** Add a mechanism to intelligently rewrite the Append-Only File to keep its size manageable over time without losing data.
  * [ ] **Improve Replication:** Make the replication system more robust with features like Partial Resynchronization (PSYNC) and better error handling during network splits.

### 🤝 Want to Help Out?

Yes, please\! Contributions are always welcome and are the best way to make this project even better. Feel free to open an issue to chat about an idea or just submit a pull request. No contribution is too small—even fixing a typo in the documentation is a huge help\!

A great place to start is our [Issues tab](https://github.com/buraksocial/IgnisDB/issues)—look for anything with the `good first issue` label\!

1.  Fork the project.
2.  Create a new branch (`git checkout -b feature/my-cool-idea`).
3.  Commit your changes (`git commit -am 'feat: Add my cool idea'`).
4.  Push your branch (`git push origin feature/my-cool-idea`).
5.  Open a Pull Request.

### 📄 License Stuff

This project uses the MIT License. You can find the boring (but important) details in the `LICENSE` file. Basically, you're free to do almost anything you want with the code.
