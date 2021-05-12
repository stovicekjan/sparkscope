-- this script should do the initial setup of the database

CREATE DATABASE sparkscope;
CREATE USER sparkscope_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE sparkscope TO sparkscope_user;
ALTER DATABASE sparkscope OWNER sparkscope_user;



-- optimize
CREATE INDEX idx_stage__app_id ON stage (app_id);