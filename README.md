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
* **"How do databases not lose my data when I turn them off?"** We'll tackle persistence by implementing a "snapshotting" feature. Think of it like taking a perfect photograph of everything in the database at a specific moment and saving that photo to a file. If the power goes out, you just load up the last photo!
* **"How do you build big, complex things without losing your mind?"** Big projects can be intimidating. We'll see how to break down a complex system into small, manageable pieces that are easy to reason about and even easier to build upon. It’s like building with LEGOs—each piece has a purpose and connects neatly to the others.

Basically, if you're tired of just *using* tools and want to feel the thrill of *building* them, you're in exactly the right place.

### 🏛️ How It Works: A Quick Tour

IgnisDB has a pretty straightforward and clean design, which is great because it means it's easy to understand. When a client sends a command, here's the epic journey it takes through our system:

```

[Client] ---\> [TCP Server] ---\> [Protocol Parser] \<---\> [Storage Engine]
^                |                   |                      |
|          (The Bouncer)     (The Translator)         (The Librarian)
|                                                           |
\+-----------------------------------------------------------+
|
[Response]

````

1.  **The TCP Server (The Front Door)**: This is our bouncer. It stands at the door, listening for new clients who want to connect. For every new friend that knocks, it says "Welcome!" and starts a new, dedicated task to listen to their requests, so everyone gets attention without waiting in line.
2.  **The Protocol Parser (The Translator)**: Computers love to talk in bytes, but humans prefer words! This part is our universal translator. It takes the raw, garbled data from the client and translates it into clean commands and arguments that the rest of our application can easily understand, like `GET my_key`.
3.  **The Storage Engine (The Brains)**: This is the heart and soul of the database. It takes the translated commands and actually does the work—storing data, fetching it, deleting it, and so on. It's also the super-organized librarian who knows exactly where every piece of information is and uses a special `Lock` to make sure that even if two people ask for the same book at once, there's no chaos.
4.  **The Response (Sending it back!)**: Once the engine has an answer (or a confirmation), the parser formats it back into a response the client can understand and sends it back over the wire. Job done!

### 🚀 What's Inside? (The Cool Features)

* **Totally Async & Efficient**: Thanks to `asyncio`, IgnisDB can juggle tons of connections at once without breaking a sweat. It's super efficient because it doesn't wait around. While it's waiting for a slow client, it happily serves other, faster ones.
* **Super Fast In-Memory Storage**: We keep everything in RAM (a simple Python dictionary, to be exact), which means reading and writing data is lightning fast. It's like having all your tools right on your workbench instead of in a shed out back. Perfect for high-speed caching!
* **Simple, Human-Readable Protocol**: The way you "talk" to the database is just plain text. No complicated binary stuff. This means you can even use a tool like `telnet` to connect and manually type commands to see how it works. It's awesome for debugging and learning.
* **Durable Data Persistence**: Don't worry about losing everything if the server restarts. IgnisDB can periodically save a "snapshot" of all its data to a file. It's a simple and effective way to get some peace of mind.
* **Automatic Key Expiration (TTL)**: You can set keys to automatically disappear after a certain amount of time. It's a "this message will self-destruct" feature for your data! Super useful for things like session tokens or temporary cache entries that you don't want hanging around forever.
* **Guaranteed Concurrency Safe**: Even if multiple clients try to write data at the exact same time, we use `asyncio.Lock` to enforce a "one at a time, please!" rule for changing data. This prevents messy collisions and ensures your data stays consistent.
* **Clean and Tidy Code**: We've intentionally kept the project split into logical parts. This makes the code easy to read, find what you're looking for, and, most importantly, makes it a breeze to add your own cool features.

### 🛠️ The Tech We're Using

* **Language**: Python 3.9+
* **Core Library**: `asyncio`
* **Testing**: `pytest` and `pytest-asyncio`

### ⚙️ Get It Running!

1.  **Clone the project:**
    ```bash
    git clone https://github.com/buraksocial/IgnisDB.git
    cd IgnisDB
    ```
2.  **Install the stuff it needs:**
    ```bash
    # It's always a good idea to use a virtual environment!
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`

    # Now, install the packages
    pip install -r requirements.txt
    ```
3.  **Fire it up!**
    ```bash
    python ignisdb_server.py
    ```
    The server should now be running on `127.0.0.1` at port `6380`. You'll see a log message confirming it!

4.  **Talk to it:**
    Open a brand new terminal window and use `telnet` to connect:
    ```bash
    telnet 127.0.0.1 6380
    ```
    Now you have a direct line to the database. You can start sending commands!
    ```
    # Set a key
    SET name 'John Doe'

    # Get it back
    GET name
    > 'John Doe'

    # Set a key that will vanish in 10 seconds
    SET score 100 EX 10

    # Try to get it after 10 seconds... it'll be gone!
    GET score
    > (nil)
    ```

### Protocol Cheatsheet

Here's how you can talk to IgnisDB. Just send these commands as plain text.

| Command         | What it does                                  | Example Usage           |
| --------------- | --------------------------------------------- | ----------------------- |
| `SET key value` | Stores a value with a key.                    | `SET user:1 'Alice'`    |
| `GET key`       | Gets the value for a key.                     | `GET user:1`            |
| `DELETE key`    | Deletes a key and its value.                  | `DELETE user:1`         |
| `EXPIRE key secs` | Makes a key disappear after some seconds.     | `EXPIRE user:1 30`      |
| `SET ... EX secs` | A handy shortcut to set a key with an expiration time. | `SET session:foo bar EX 60` |

### 🧪 Running the Tests

Want to make sure everything is working as it should? Or maybe you've added a new feature and want to be sure you didn't break anything? Just run the tests.
```bash
pytest
````

### 🗺️ What's Next? (The Roadmap)

This project is always a work in progress\! Here are a few ideas for cool features we could add next. They are great starting points if you want to contribute\!

  - [ ] **Add more data types, like Lists and Hashes.** This would make the database much more powerful, allowing you to do more than just store simple strings.
  - [ ] **Create a different way to save data (Append-Only File).** This is an alternative to snapshotting where every single write command is logged to a file. It's great for durability\!
  - [ ] **Build a simple replication system (master-slave).** This is a huge step towards a "real" database. It means you can have a backup database ready to take over if the main one has a problem.
  - [ ] **Add support for transactions (`MULTI`/`EXEC`).** This would let you group multiple commands together so they all run at once, or not at all.
  - [ ] **Create a tool to see how fast it really is\!** A simple benchmarking script would be awesome to measure performance and see how our changes impact speed.

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
