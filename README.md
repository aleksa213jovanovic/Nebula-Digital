## RUN THE APP
Navigate to `/NebulaDigital` dir and run `docker compose up --build` command

## Required Open Ports

The following ports must be open for the application to function correctly:

- **8081**: This is used by the `transaction-app`
- **5438**: This is used by the `transaction-db`
- **8082**: This is used by the `user-app`
- **5437** This is used by the `user-db`
- **4222** This is used by the `nats`
- **8222** This is used by the `nats`


## Available Routes

Here are the routes that can be accessed in `user-app`:

- `POST /user-service/user/create`: expecting { email: } body
- `GET /user-service/user/balance`: expecting { email: , balance: } body

Here are the routes that can be accessed in `transaction-app`:
- `POST /transaction-service/user/credit`: expecting { user_id: , amount: } body
- `POST /transaction-service/user/transfer`: expecting { from_user_id: , to_user_id: ,amount_to_transfer: } body 
