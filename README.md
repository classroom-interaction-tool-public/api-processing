# CIT - Server

## Getting Started

TODO: Add a section about running dev scripts

The server requires a connection to the Mongo DB before it's able to start serving.
Visit the repository `mvp-database`, download it, and run it before starting the server.

After the database is up and running, install the packages used by the student client by issuing the command:

```bash
npm i
```

Next, create a `.env` file using the `.env.example` file as a template.
An example `.env` file might look like this:

```javascript
MONGODB_URI=mongodb://localhost:30000
MONGODB_DB_NAME=citdb
PORT=5000
JWT_SECRET=H7c9pxtcGfrRJYKLsa3g
COOKIE_SECRET=DmxTw7EXW4Zi
```

Then run the server issuing the command:

```bash
npm run dev
```

The server should start running at http://localhost:5000 by default, or at the specified PORT provided in a .env file (see `.env.example`).

## Swagger

Swagger UI for the project can be reached at http://localhost:5000/documentation after the server has successfully started.
