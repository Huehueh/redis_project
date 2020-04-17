# Short description

The aim of this project is to connect to redis and sent given database index on it. Information about redis is acquired from the sentinels. User should give addresses of all the sentinels, tiemout for the connection and redis database index that shall be setup on redis.

### Running the application

User has to run the app with the following arguments:
1) list of tuples ip_address and port in format ip:port (1 tuple minimum)
2) timeout (in seconds) for the redis connection (float)
3) redis database index (int)