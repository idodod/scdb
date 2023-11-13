# SCDB
## _Solid Core Database System_

Solid Core Database is a basic key-value database engine that is true to the fundamentals, fast, and easy to access.

## Features

- `scdb` is a lightweight and fast key-value storage engine that stands out for its simplicity and speed. It's modeled after the Bitcask storage architecture, renowned for its efficiency in write operations.

- The core design principle behind `scdb` (and Bitcask) is influenced by log-structured filesystems. In this approach, all changes are appended to the end of a log, making writes exceptionally fast because they're sequential. This design is not only quick but also ensures the integrity of your data: even in the event of a system crash, you can recover the data easily thanks to this append-only method.

- Additionally, `scdb` incorporates a process known as log file merging. Over time, as your application writes more data, the number of log files will naturally increase. Log file merging is a technique that combines these files to prevent fragmentation and maintain performance. It's like housekeeping for your data, ensuring that your storage remains organized and efficient.

- So, if you're developing an application that requires speedy data writes and a simple yet robust storage solution, scdb might just be the engine you're looking for. It's built to give developers like you a hassle-free experience with the storage and retrieval of key-value pairs.