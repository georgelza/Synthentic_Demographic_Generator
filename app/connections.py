#######################################################################################################################
#
#
#   Project         :   Generic Data generator - OOP Database Connection Classes
#
#   File            :   connections.py
#
#   Description     :   Object-oriented database connection and operation classes
#   
#   Created         :   06 Aug 2025
#
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.2"
__copyright__   = "Copyright 2025, - George Leonard"


import json
import sys, os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Union

try:
    import pymongo
    print("MongoDB, Module Import Successful")
    
except ImportError as error:
    print("MongoDB, Module Import Error")
    print(error)
    sys.exit(0)
    
try:
    import psycopg2
    from psycopg2.extras import Json, execute_values
    from psycopg2 import sql
    print("PostgreSQL, Module Import Successful")
    
except ImportError as error:
    print("PostgreSQL, Module Import Error")
    print(error)
    sys.exit(0)

try:
    import redis
    print("Redis, Module Import Successful")
    
except ImportError as error:
    print("Redis, Module Import Error")
    print(error)
    sys.exit(0)
    

class DatabaseConnectionError(Exception):
    
    """Custom exception for database connection errors"""
    
    pass
#end DatabaseConnectionError


class DatabaseOperationError(Exception):
    
    """Custom exception for database operation errors"""
    
    pass
#end DatabaseOperationError


class DatabaseConnection(ABC):
    
    """Abstract base class for database connections"""
    
    def __init__(self, config_params: Dict[str, Any], mylogger):
        self.config_params  = config_params
        self.mylogger         = mylogger
        self.connection     = None
    #end __init__
    
        
    @abstractmethod
    def connect(self):
        
        """Establish database connection"""
        
        pass
    #end connect
    
        
    @abstractmethod
    def disconnect(self):
        
        """Close database connection"""
        
        pass
    #end disconnect
    
        
    @abstractmethod
    
    def insert_single(self, data: Dict[str, Any], **kwargs) -> Any:
        
        """Insert single document/record"""
        
        pass
    #end insert_single
    
        
    @abstractmethod
    
    def insert_multiple(self, data: List[Dict[str, Any]], **kwargs) -> Any:
        
        """Insert multiple documents/records"""
        
        pass
    #end insert_multiple
#end DatabaseConnection


class MongoDBConnection(DatabaseConnection):
    
    """MongoDB connection and operations class"""
    
    def __init__(self, 
                 config_params: Dict[str, Any], 
                 mylogger):
        
        super().__init__(config_params, mylogger)
        self.client      = None
        self.database    = None
        self.collections = {}
    #end __init__
     
        
    def connect(self) -> bool:
        
        """Establish MongoDB connection"""
        
        try:
            if self.config_params["MONGO_ROOT"] == "mongodb":
                if self.config_params["MONGO_USERNAME"] != "": 
                    mongo_uri = f'{self.config_params["MONGO_ROOT"]}://{self.config_params["MONGO_USERNAME"]}:{self.config_params["MONGO_PASSWORD"]}@{self.config_params["MONGO_HOST"]}:{int(self.config_params["MONGO_PORT"])}/?{self.config_params["MONGO_DIRECT"]}'
                
                else:
                    mongo_uri = f'{self.config_params["MONGO_ROOT"]}://{self.config_params["MONGO_HOST"]}:{int(self.config_params["MONGO_PORT"])}/?{self.config_params["MONGO_DIRECT"]}'

                #end if
            else:
                if self.config_params["MONGO_USERNAME"] != "": 
                    mongo_uri = f'{self.config_params["MONGO_ROOT"]}://{self.config_params["MONGO_USERNAME"]}:{self.config_params["MONGO_PASSWORD"]}@{self.config_params["MONGO_HOST"]}'

                #end if
            #end if
                                
            self.mylogger.debug(f'MongoDB Connection URI: {mongo_uri}')

            try:
                self.client = pymongo.MongoClient(mongo_uri)
                self.client.server_info()  # Force connection test
                self.database = self.client[self.config_params["MONGO_DATASTORE"]]
                
                self.mylogger.info('MongoDB connection established to: {dbstore}'.format(
                    dbstore=self.config_params["MONGO_DATASTORE"]
                ))
                return True
                
            except pymongo.errors.ServerSelectionTimeoutError as err:
                self.mylogger.error('MongoDB connection failed: {host} {dbstore} {err}'.format(
                    host     = self.config_params["MONGO_HOST"],
                    dbstore  = self.config_params["MONGO_DATASTORE"],
                    err      = err
                ))


                raise DatabaseConnectionError(f"MongoDB connection failed: {err}")
            
            #end try    
        except Exception as err:
            self.mylogger.error('MongoDB connection error: {host} {dbstore} {err}'.format(
                host     = self.config_params["MONGO_HOST"],
                dbstore  = self.config_params["MONGO_DATASTORE"],
                err      = err
            ))

            raise DatabaseConnectionError(f"MongoDB connection error: {err}")
        #end try
    #end connect
    
    
    def get_collection(self, collection_name: str):
        
        """Get or create collection reference"""
        
        if collection_name not in self.collections:
            if self.database is None:
                raise DatabaseConnectionError("Database not connected")
            
            #end if
            self.collections[collection_name] = self.database[collection_name]
            self.mylogger.info('MongoDB collection reference created: {host} {dbstore} {collection_name}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                collection_name = collection_name
            ))            
        return self.collections[collection_name]
    #end get_collection
    
    
    def insert_single(self, data: Dict[str, Any], collection_name: str, **kwargs) -> Any:
        
        """Insert single document into MongoDB collection"""
        
        try:
            collection = self.get_collection(collection_name)
            result = collection.insert_one(data)
            
            self.mylogger.debug('MongoDB single document inserted into: {collection_name} {result_id}'.format(
                host      = collection_name,
                result_id = result.inserted_id
            ))
            
            return result.inserted_id
            
        except Exception as err:
            self.mylogger.error('MongoDB single insert error in: {host} {dbstore} {collection_name} {err}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                collection_name = collection_name,
                err             = err
            ))
            raise DatabaseOperationError(f"MongoDB single insert failed: {err}")
        #end try
    #end insert_single
    
    
    def insert_multiple(self, data: List[Dict[str, Any]], collection_name: str, **kwargs) -> List[Any]:
        
        """Insert multiple documents into MongoDB collection"""
        
        try:
            if not data:
                return []
                
            #end if
            collection = self.get_collection(collection_name)
            result     = collection.insert_many(data)
            
            self.mylogger.debug('MongoDB inserted : {result_id} documents into {collection_name}'.format(
                result_id       = len(result.inserted_ids),
                collection_name = collection_name
            ))
            
            return result.inserted_ids
            
        except Exception as err:
            self.mylogger.error('MongoDB multiple insert error in : {host} {dbstore} {collection_name} {err}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                collection_name = collection_name,
                err             = err
            ))
            raise DatabaseOperationError(f"MongoDB multiple insert failed: {err}")
        #end try
    #end insert_multiple
    
    
    def disconnect(self):
        
        """Close MongoDB connection"""
        
        if self.client:
            self.client.close()
            self.mylogger.error('MongoDB connection closed: {host} {dbstore}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"]
            ))
        #end if
    #end disconnect
#end MongoConnection


class PostgreSQLConnection(DatabaseConnection):
    
    """PostgreSQL connection and operations class"""
    
    def __init__(self, config_params: Dict[str, Any], mylogger):
        super().__init__(config_params, mylogger)
        
    #end __init__
    
    
    def connect(self) -> bool:
        
        """Establish PostgreSQL connection"""
        
        try:
            self.connection = psycopg2.connect(
                host     = self.config_params["POSTGRES_HOST"],
                database = self.config_params["POSTGRES_DB"],
                user     = self.config_params["POSTGRES_USER"],
                password = self.config_params["POSTGRES_PASSWORD"]
            )
            
            self.mylogger.debug('PostgreSQL connection established to: {host} {dbstore}'.format(
                connection    = self.config_params["POSTGRES_HOST"],
                dbstore       = self.config_params["POSTGRES_DB"]
            ))
            return True
            
        except Exception as err:
            self.mylogger.error('PostgreSQL connection failed: {dbstore} {err}'.format(
                dbstore       = self.config_params["POSTGRES_DB"],
                err           = err
            ))
            raise DatabaseConnectionError(f"PostgreSQL connection failed: {err}")
        #end try
    #end connect
    
    
    @contextmanager
    def get_cursor(self):
        
        """Context manager for database cursor"""
        
        if not self.connection:
            raise DatabaseConnectionError("PostgreSQL not connected")
        
        #end if
        
        cursor = self.connection.cursor()
        
        try:
            yield cursor
            self.connection.commit()
            
        except Exception:
            self.connection.rollback()
            raise
        
        finally:
            cursor.close()
        #end try    
    #end get_cursor
    
    
    def insert_single(self, 
                      data:         Dict[str, Any], 
                      table_name:   str = "documents", 
                      unique_id:    Optional[str] = None, **kwargs) -> Any:
        
        """
        Insert single JSON document into PostgreSQL table
        
        Args:
            data:       Dictionary containing the JSON data
            table_name: Name of the table (default: 'documents')
            unique_id:  Unique identifier (optional)
            
        Returns:
            Record ID and creation timestamp tuple
        """
        
        try:
            with self.get_cursor() as cursor:
                json_string = json.dumps(data)
                
                if unique_id is not None:
                    query = sql.SQL("""
                        INSERT INTO public.{table_name} (uniqueId, data) 
                        VALUES (%s, %s)
                        RETURNING id, created_at;
                    """).format(table_name=sql.Identifier(table_name))
                    
                    cursor.execute(query, (unique_id, Json(json_string)))
                    
                else:
                    query = sql.SQL("""
                        INSERT INTO public.{table_name} (data) 
                        VALUES (%s)
                        RETURNING id, created_at;
                    """).format(table_name=sql.Identifier(table_name))
                    
                    cursor.execute(query, (Json(json_string),))
                
                #end if
                
                result                = cursor.fetchone()
                record_id, created_at = result
                
                self.mylogger.debug('PostgreSQL single record inserted into {table_name} with ID: {record_id}'.format(
                    table_name  = table_name,
                    record_id   = record_id
                ))
                return result
            #end with    
                
        except Exception as err:
            self.mylogger.error('PostgreSQL single insert error in {table_name}: {err}'.format(
                table_name    = table_name,
                err           = err
            ))            
            
            raise DatabaseOperationError(f"PostgreSQL single insert failed: {err}")
        #end try
    #end insert_single
        
    
    def insert_multiple(self, 
                        data:               List[Dict[str, Any]], 
                        table_name:         str = "families", 
                        extract_unique_id:  bool = False, **kwargs) -> None:
        
        """
        Insert multiple JSON documents into PostgreSQL table
        
        Args:
            data:               List of dictionaries to insert
            table_name:         Name of the table
            extract_unique_id:  Whether to extract uniqueId from JSON data
        """
        
        try:
            if not data:
                return
            
            #end if
                
            with self.get_cursor() as cursor:
                json_array_string = json.dumps(data)
                
                if extract_unique_id:
                    query = sql.SQL("""
                        INSERT INTO {table_name} (uniqueId, data)
                        SELECT (jsonb_array_elements(%s)->>'uniqueId'), jsonb_array_elements(%s)
                        ON CONFLICT (uniqueId) DO NOTHING;
                    """).format(table_name=sql.Identifier(table_name))
                    
                    cursor.execute(query, (json_array_string, json_array_string))
                    
                else:
                    query = sql.SQL("""
                        INSERT INTO {table_name} (data) 
                        SELECT jsonb_array_elements(%s); 
                    """).format(table_name=sql.Identifier(table_name))
                    
                    cursor.execute(query, (json_array_string,))
                
                #end if
                
                self.mylogger.debug('PostgreSQL multiple record inserted into {table_name}'.format(
                    table_name  = table_name
                ))
            #end with
        except Exception as err:
            self.mylogger.error('PostgreSQL multiple insert error in {table_name}: {err}'.format(
                table_name    = table_name,
                err           = err
            ))  
            
            raise DatabaseOperationError(f"PostgreSQL multiple insert failed: {err}")
        #end with
    #end insert_multiple
    
    
    def insert_flattened(self, 
                         data:          Dict[str, Any], 
                         table_name:    str = "documents", 
                         **kwargs) -> Any:
        
        """
        Insert flattened JSON data into PostgreSQL table columns
        
        Args:
            data: Dictionary containing the data to flatten
            table_name: Name of the table
            
        Returns:
            Result of the insert operation
        """
        
        try:
            with self.get_cursor() as cursor:
                # Build dynamic SQL based on data keys
                columns = ', '.join(data.keys())
                placeholders = ', '.join([f'%({key})s' for key in data.keys()])
                
                sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) RETURNING id;"
                
                cursor.execute(sql_query, data)
                result = cursor.fetchone()
                                
                self.mylogger.debug('PostgreSQL flattened record inserted into {table_name} with ID: {result[0]}'.format(
                    table_name       = table_name,
                    result           = result[0]
                ))
                
                return result
            #end with
        except Exception as err:
            self.mylogger.error('PostgreSQL flattened insert error in {table_name}: {err}'.format(
                table_name    = table_name,
                err           = err
            ))
            
            raise DatabaseOperationError(f"PostgreSQL flattened insert failed: {err}")
        #end try
    #end insert_flattened
    
    
    def disconnect(self):
        
        """Close PostgreSQL connection"""
        
        if self.connection:
            self.connection.close()
            self.mylogger.info('PostgreSQL connection closed')
        #end if
    #end disconnect
# end PostgreSQLConnection


class RedisConnection(DatabaseConnection):
    
    """Redis connection and operations class"""
    
    def __init__(self, 
                 config_params: Dict[str, Any], 
                 mylogger):
        
        super().__init__(config_params, mylogger)
        
        self.client = None
    #end __init__
        
    
    def connect(self) -> bool:
        
        """Establish Redis connection"""
        
        try:
            # Build Redis connection parameters
            redis_params = {
                'host':                     self.config_params["REDIS_HOST"],
                'port':                     self.config_params.get("REDIS_PORT", 6379),
                'db':                       self.config_params.get("REDIS_DB",   0),
                'ssl':                      self.config_params.get("REDIS_SSL",   False),
                'decode_responses':         True,
                'socket_connect_timeout':   5,
                'socket_timeout':           5,
                'retry_on_timeout':         True
            }
            
            self.mylogger.debug('Redis Connection {connection}'.format(
                connection = redis_params
            ))

            # Add password if provided
            if self.config_params.get("REDIS_PASSWORD"):
                redis_params['password'] = self.config_params["REDIS_PASSWORD"]

                self.mylogger.debug('Redis Password!!!! Configured')
            #end if
            
            
            # Add SSL support if specified
            if self.config_params.get("REDIS_SSL", False):
                redis_params['ssl'] = True
                
                # Handle certificate verification
                ssl_verify = self.config_params.get("REDIS_SSL_VERIFY", True)
                if ssl_verify:
                    redis_params['ssl_cert_reqs'] = 'required'
                    
                    # Read CA certificate file if provided
                    ca_cert_path = self.config_params.get("REDIS_SSL_CA_CERT")
                    if ca_cert_path:
                        try:
                            # Verify file exists and is readable
                            if not os.path.exists(ca_cert_path):
                                raise FileNotFoundError(f"CA certificate file not found: {ca_cert_path}")
                            
                            #end if
                            
                            redis_params['ssl_ca_certs'] = ca_cert_path

                            self.mylogger.debug('Using CA certificate: {ca_cert_path}'.format(
                                ca_cert_path           = ca_cert_path
                            ))
                        except Exception as err:
                            self.mylogger.error('Error reading CA certificate {ca_cert_path}: {err}'.format(
                                ca_cert_path = ca_cert_path
                            ))
                            raise DatabaseConnectionError(f"SSL CA certificate error: {err}")

                        #end try
                    #end if
                    
                    # Read client certificate file if provided
                    client_cert_path = self.config_params.get("REDIS_SSL_CLIENT_CERT")
                    if client_cert_path:
                        try:
                            if not os.path.exists(client_cert_path):
                                raise FileNotFoundError(f"Client certificate file not found: {client_cert_path}")
                            
                            #end if
                            
                            redis_params['ssl_certfile'] = client_cert_path
                            
                            self.mylogger.debug('Using client certificate: {client_cert_path}'.format(
                                ca_cert_path = ca_cert_path
                            ))

                        except Exception as err:
                            self.mylogger.error('Error reading client certificate {client_cert_path}: {err}'.format(
                                client_cert_path = client_cert_path,
                                err              = err
                            ))
                            
                            raise DatabaseConnectionError(f"SSL client certificate error: {err}")
                        #end try
                    #end if
                    
                    
                    # Read client private key file if provided
                    client_key_path = self.config_params.get("REDIS_SSL_CLIENT_KEY")
                    if client_key_path:
                        try:
                            if not os.path.exists(client_key_path):
                                raise FileNotFoundError(f"Client key file not found: {client_key_path}")
                            
                            #end if
                            
                            redis_params['ssl_keyfile'] = client_key_path
                            
                            self.mylogger.debug('Using client key: {client_key_path}'.format(
                                client_key_path = client_key_path
                            ))

                        except Exception as err:
                            self.mylogger.debug('Error reading client key {client_key_path}: {err}'.format(
                                client_key_path = client_key_path,
                                err             = err
                            ))
                            
                            raise DatabaseConnectionError(f"SSL client key error: {err}")
                        #end try
                    #end if
                else:
                    # Disable certificate verification (development only!)
                    redis_params['ssl_cert_reqs'] = None
                    self.mylogger.warning('SSL certificate verification is DISABLED - not recommended for production!')

                #end if
            else:
                redis_params['ssl'] = False
            #end if
            
            self.client = redis.Redis(**redis_params)

            self.mylogger.debug('Redis Connection: {client}'.format(
                client = self.client
            ))
            
            # Test connection
            self.client.ping()
            
            self.mylogger.info('Redis connection established to: {host}:{port}/{db}'.format(
                host = self.config_params["REDIS_HOST"],
                port = self.config_params.get("REDIS_PORT", 6379), 
                db   = self.config_params.get("REDIS_DB",   0)
            ))
            
            return True
            
        except Exception as err:
            self.mylogger.error('Redis connection failed: {host}:{port} {err}'.format(
                host = self.config_params["REDIS_HOST"],
                port = self.config_params["REDIS_PORT"],
                err  = err
            ))
            
            raise DatabaseConnectionError(f"Redis connection failed: {err}")
        #end try
    #end connect
    
    
    def _generate_key(self, 
                      collection_name:  str, 
                      key_field:        str, 
                      data:             Dict[str, Any]) -> str:
        
        """
        Generate Redis key with collection prefix
        
        Args:
            collection_name: Collection name to use as prefix
            key_field:       Field name to extract from data as key suffix
            data:            Dictionary containing the data
            
        Returns:
            Formatted Redis key: collection_name:field_value
        """
        
        if key_field not in data:
            raise ValueError(f"Key field '{key_field}' not found in data")
        
        #end if
        
        key_value = data[key_field]
        
        return f"{collection_name}:{key_value}"
    #end _generate_key
    
    
    def insert_single(self, 
                      data:             Dict[str, Any], 
                      collection_name:  str, 
                      key_field:        str, 
                      ttl:              Optional[int] = None, 
                      **kwargs) -> str:
        
        """
        Insert single JSON document into Redis
        
        Args:
            data:               Dictionary containing the JSON data
            collection_name:    Collection name to use as key prefix
            key_field:          Field name to use for key generation
            ttl:                Time to live in seconds (optional)
            
        Returns:
            Generated Redis key
        """
        
        try:
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            redis_key   = self._generate_key(collection_name, key_field, data)
            json_string = json.dumps(data, default=str)
            
            if ttl:
                self.client.setex(redis_key, ttl, json_string)
            
            else:
                self.client.set(redis_key, json_string)
            
            #end if
        
        
            self.mylogger.debug('Redis single document inserted with key: {redis_key}'.format(
                redis_key = redis_key
            ))
            
            return redis_key
            
        except Exception as err:
            self.mylogger.error('Redis single insert error for {collection_name}: {err}'.format(
                collection_name = collection_name,
                err             = err
            ))
            
            raise DatabaseOperationError(f"Redis single insert failed: {err}")
        #end try
    #end insert_single
    
    
    def insert_multiple(self, 
                        data:               List[Dict[str, Any]], 
                        collection_name:    str,
                        key_field:          str, 
                        ttl:                Optional[int] = None, 
                        **kwargs) -> List[str]:
        
        """
        Insert multiple JSON documents into Redis using pipeline
        
        Args:
            data: List of dictionaries to insert
            collection_name: Collection name to use as key prefix  
            key_field: Field name to use for key generation
            ttl: Time to live in seconds (optional)
            
        Returns:
            List of generated Redis keys
        """
        
        try:
            if not data:
                return []
            
            #end if
            
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            pipe       = self.client.pipeline()
            redis_keys = []
                        
            for item in data:
                redis_key   = self._generate_key(collection_name, key_field, item)
#                json_string = json.dumps(item, default=str)
                json_string = json.dumps(item)
                
                if ttl:
                    pipe.setex(redis_key, ttl, json_string)
            
                else:
                    pipe.set(redis_key, json_string)
                
                #end if    
                redis_keys.append(redis_key)
            
            #end for
            
            pipe.execute()
            
            self.mylogger.debug('Redis inserted {count} documents into {collection_name}'.format(
                count           = len(data),
                collection_name = collection_name
            ))
            
            return redis_keys
            
        except Exception as err:
            self.mylogger.error('Redis multiple insert error for {collection_name}: {err}'.format(
                collection_name = collection_name,
                err             = err
            ))
            
            raise DatabaseOperationError(f"Redis multiple insert failed: {err}")
        #end try
    #end insert_multiple
    
    
    def get_by_key(self, 
                   redis_key: str) -> Optional[Dict[str, Any]]:
        
        """
        Retrieve document by Redis key
        
        Args:
            redis_key: Redis key to retrieve
            
        Returns:
            Dictionary containing the data or None if not found
        """
        
        try:
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            json_string = self.client.get(redis_key)
            
            if json_string:
                return json.loads(json_string)
            
            #end if
            
            return None
            
        except Exception as err:
            self.mylogger.error('Redis get error for key {redis_key}: {err}'.format(
                redis_key = redis_key,
                err       = err
            ))
            
            raise DatabaseOperationError(f"Redis get failed: {err}")
        #end try
    #end get_by_key
    
    
    def get_by_pattern(self, 
                       pattern: str) -> Dict[str, Dict[str, Any]]:
        
        """
        Retrieve multiple documents by key pattern
        
        Args:
            pattern: Redis key pattern (e.g., 'adults:*')
            
        Returns:
            Dictionary mapping keys to their JSON data
        """
        
        try:
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            keys = self.client.keys(pattern)
            
            if not keys:
                return {}
            
            #end if
            
            pipe = self.client.pipeline()
            
            for key in keys:
                pipe.get(key)
            
            #end for
            
            values = pipe.execute()
            
            result = {}
            for key, value in zip(keys, values):
                if value:
                    result[key] = json.loads(value)
                    
                #end if
            #end for
            
            self.mylogger.debug('Redis retrieved {count} documents with pattern {pattern}'.format(
                count   = len(result),
                pattern = pattern
            ))
            
            return result
            
        except Exception as err:
            self.mylogger.error('Redis pattern get error for {pattern}: {err}'.format(
                pattern = pattern,
                err     = err
            ))
            
            raise DatabaseOperationError(f"Redis pattern get failed: {err}")
        #end try
    #end get_by_pattern
    
    
    def delete_by_key(self, 
                      redis_key: str) -> bool:
        
        """
        Delete document by Redis key
        
        Args:
            redis_key: Redis key to delete
            
        Returns:
            True if deleted, False if key didn't exist
        """
        
        try:
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            result = self.client.delete(redis_key)
            
            self.mylogger.debug('Redis key {redis_key} deleted: {result}'.format(
                redis_key = redis_key,
                result    = bool(result)
            ))
            
            return bool(result)
            
        except Exception as err:
            self.mylogger.error('Redis delete error for key {redis_key}: {err}'.format(
                redis_key = redis_key,
                err       = err
            ))
            
            raise DatabaseOperationError(f"Redis delete failed: {err}")
        #end try
    #end delete_by_key
    
    
    def disconnect(self):
        
        """Close Redis connection"""
        
        if self.client:
            self.client.close()
            self.mylogger.info('Redis connection closed: {host}:{port}'.format(
                host = self.config_params["REDIS_HOST"],
                port = self.config_params["REDIS_PORT"]
            ))
        #end if
    #end disconnect
#end RedisConnection


class DatabaseManager:
    
    """Factory class for managing different database connections"""
    
    @staticmethod
    def create_connection(db_type:       str, 
                          config_params: Dict[str, Any], 
                          mylogger) -> DatabaseConnection:
        
        """
        Factory method to create appropriate database connection
        
        Args:
            db_type: Type of database ('mongodb' or 'postgresql')
            config_params: Configuration parameters
            mylogger: mylogger instance
            
        Returns:
            DatabaseConnection instance
        """

        if db_type.lower() == 'mongodb':
            conn =  MongoDBConnection(config_params,    mylogger)
        
        elif db_type.lower() == 'postgresql':
            conn =  PostgreSQLConnection(config_params, mylogger)
        
        elif db_type.lower() == 'redis':
            conn =  RedisConnection(config_params,      mylogger)
        
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        #end if
                
        return conn
    #end create_connection
#end DatabaseManager


# Convenience functions for backward compatibility (optional)
def connect_to_mongodb(config_params, 
                       collection, 
                       mylogger):
    
    """Legacy function wrapper for MongoDB connection"""
    
    mongo_conn = MongoDBConnection(config_params, mylogger)
    mongo_conn.connect()
    
    return mongo_conn.get_collection(collection)
#end connect_to_mongodb


def connect_to_postgreSQL(config_params, 
                          mylogger):
    
    """Legacy function wrapper for PostgreSQL connection"""
    
    pg_conn = PostgreSQLConnection(config_params, mylogger)
    pg_conn.connect()
    
    return pg_conn.connection
#end connect_to_postgreSQL


def connect_to_redis(config_params, 
                     mylogger):
    
    """Legacy function wrapper for Redis connection"""
    
    redis_conn = RedisConnection(config_params, mylogger)
    redis_conn.connect()
    
    return redis_conn.client
#end connect_to_redis