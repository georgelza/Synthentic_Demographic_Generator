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
#                   :   18 Aug 2025 - Added Kafka as destitation
#
########################################################################################################################
__author__      = "Generic Data playground"
__email__       = "georgelza@gmail.com"
__version__     = "0.3"
__copyright__   = "Copyright 2025, - George Leonard"


import json, socket, time
import sys, os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Union

try:
    import pymongo
    from pymongo.errors import ServerSelectionTimeoutError, ConnectionFailure
    print("MongoDB, Module Import Successful")
    
except ImportError as err:
    print("MongoDB, Module Import Error {err}")
    sys.exit(1)
    
try:
    import psycopg2
    from psycopg2.extras import Json, execute_values
    from psycopg2 import sql
    from psycopg2.errors import Error as PostgreSQLError
    print("PostgreSQL Module Import Successful")
    
except ImportError as err:
    print("PostgreSQL, Module Import Error {err}")
    sys.exit(1)

try:
    import redis
    from redis.exceptions import RedisError, ConnectionError as RedisConnectionError
    print("Redis, Module Import Successful")
    
except ImportError as err:
    print("Redis, Module Import Error {err}")
    sys.exit(1)
    
try:
    from confluent_kafka import Producer, KafkaError, KafkaException
    print("Confluent Kafka, Module Import Successful")
    
except ImportError as err:
    print("Confluent Kafka, Module Import Error {err}")
    sys.exit(1)


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
        self.mylogger       = mylogger
        self.connection     = None
        self._is_connected  = False
    #end __init__
    
        
    @property
    def is_connected(self) -> bool:
        
        """Check if database is connected"""
        
        return self._is_connected
    #end is_connected
    
    
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
            mongo_uri = self._build_mongo_uri()                                
            self.mylogger.debug(f'MongoDB Connection URI: {mongo_uri}')

            self.client = pymongo.MongoClient(
                mongo_uri,
                serverSelectionTimeoutMS=5000,          # 5 second timeout
                connectTimeoutMS        =10000,         # 10 second connection timeout
                socketTimeoutMS         =20000          # 20 second socket timeout
            )
            self.client.server_info()  # Force connection test
            self.database = self.client[self.config_params["MONGO_DATASTORE"]]

            self._is_connected = True
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
            
        except ConnectionFailure as err:
            self.mylogger.error('MongoDB connection failed: {host} {err}'.format(
                host     = self.config_params["MONGO_HOST"],
                err      = err
            ))
            raise DatabaseConnectionError(f"MongoDB connection failed: {err}")
            
        except Exception as err:
            self.mylogger.error('MongoDB connection error: {host} {dbstore} {err}'.format(
                host     = self.config_params["MONGO_HOST"],
                dbstore  = self.config_params["MONGO_DATASTORE"],
                err      = err
            ))
            raise DatabaseConnectionError(f"MongoDB connection error: {err}")
        #end try
    #end connect
    
    
    def _build_mongo_uri(self) -> str:
    
        """Build MongoDB connection URI"""
        root        = self.config_params["MONGO_ROOT"]
        host        = self.config_params["MONGO_HOST"]
        port        = int(self.config_params["MONGO_PORT"])
        username    = self.config_params.get("MONGO_USERNAME", "")
        password    = self.config_params.get("MONGO_PASSWORD", "")
        direct      = self.config_params.get("MONGO_DIRECT", "")
        
        if root == "mongodb":
            if username:
                uri = f'{root}://{username}:{password}@{host}:{port}/?{direct}'
            else:
                uri = f'{root}://{host}:{port}/?{direct}'
        else:  # mongodb+srv
            if username:
                uri = f'{root}://{username}:{password}@{host}'
            else:
                uri = f'{root}://{host}'
        
        return uri
    #end _build_mongo_uri
    
    
    def get_collection(self, store_name: str):
        
        """Get or create collection reference"""
        
        if store_name not in self.collections:
            if self.database is None:
                raise DatabaseConnectionError("Database not connected")
            
            #end if
            self.collections[store_name] = self.database[store_name]
            self.mylogger.info('MongoDB collection reference created: {host} {dbstore} {store_name}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                store_name = store_name
            ))            
        return self.collections[store_name]
    #end get_collection
    
    
    def insert_single(self, data: Dict[str, Any], store_name: str, **kwargs) -> Any:
        
        """Insert single document into MongoDB collection"""
        
        try:
            collection = self.get_collection(store_name)
            result = collection.insert_one(data)
            
            self.mylogger.debug('MongoDB single document inserted into: {store_name} {result_id}'.format(
                host      = store_name,
                result_id = result.inserted_id
            ))
            
            return result.inserted_id
            
        except Exception as err:
            self.mylogger.error('MongoDB single insert error in: {host} {dbstore} {store_name} {err}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                store_name = store_name,
                err             = err
            ))
            raise DatabaseOperationError(f"MongoDB single insert failed: {err}")
        #end try
    #end insert_single
    
    
    def insert_multiple(self, data: List[Dict[str, Any]], store_name: str, **kwargs) -> List[Any]:
        
        """Insert multiple documents into MongoDB collection"""
        
        try:
            if not data:
                return []
                
            #end if
            collection = self.get_collection(store_name)
            result     = collection.insert_many(data)
            
            self.mylogger.debug('MongoDB inserted : {result_id} documents into {store_name}'.format(
                result_id       = len(result.inserted_ids),
                store_name = store_name
            ))
            
            return result.inserted_ids
            
        except Exception as err:
            self.mylogger.error('MongoDB multiple insert error in : {host} {dbstore} {store_name} {err}'.format(
                host            = self.config_params["MONGO_HOST"],
                dbstore         = self.config_params["MONGO_DATASTORE"],
                store_name = store_name,
                err             = err
            ))
            raise DatabaseOperationError(f"MongoDB multiple insert failed: {err}")
        #end try
    #end insert_multiple
    
    
    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs) -> Any:
        
        """Universal insert method that routes to single or multiple insert"""
        
        if isinstance(data, list):
            if len(data) > 1:
                return self.insert_multiple(data, **kwargs)
            
            elif len(data) == 1:
                return self.insert_single(data[0], **kwargs)
            
            else:
                return []
        else:
            return self.insert_single(data, **kwargs)
        #end if
    #end insert
    
    
    def health_check(self) -> bool:
        
        """Perform MongoDB health check"""
        
        try:
            if self.client:
                self.client.server_info()
                
                return True
        except Exception as err:
            self.mylogger.error(f'MongoDB health check failed: {err}')
            self._is_connected = False
        
        return False
    #end health_check
    
    
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
                host            = self.config_params["POSTGRES_HOST"],
                database        = self.config_params["POSTGRES_DB"],
                user            = self.config_params["POSTGRES_USER"],
                password        = self.config_params["POSTGRES_PASSWORD"],
                connect_timeout = 10  # 10 second timeout
            )

            self._is_connected = True
            self.mylogger.debug('PostgreSQL connection established to: {host} {dbstore}'.format(
                connection    = self.config_params["POSTGRES_HOST"],
                dbstore       = self.config_params["POSTGRES_DB"]
            ))
            return True
            
        except PostgreSQLError as err:
            self.mylogger.error('PostgreSQL connection failed: {host} {err}'.format(
                host = self.config_params["POSTGRES_DB"],
                err  = err
            ))
            raise DatabaseConnectionError(f"PostgreSQL connection failed: {err}")
        
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
                      store_name:   str = "documents", 
                      unique_id:    Optional[str] = None, 
                      **kwargs) -> Any:
        
        """
        Insert single JSON document into PostgreSQL table
        
        Args:
            data:       Dictionary containing the JSON data
            store_name: Name of the table (default: 'documents')
            unique_id:  Unique identifier (optional)
            
        Returns:
            Record ID and creation timestamp tuple
        """
        
        try:
            with self.get_cursor() as cursor:
                json_string = json.dumps(data)
                
                if unique_id is not None:
                    query = sql.SQL("""
                        INSERT INTO public.{store_name} (uniqueId, data) 
                        VALUES (%s, %s)
                        RETURNING id, created_at;
                    """).format(store_name=sql.Identifier(store_name))
                    
                    cursor.execute(query, (unique_id, Json(json_string)))
                    
                else:
                    query = sql.SQL("""
                        INSERT INTO public.{store_name} (data) 
                        VALUES (%s)
                        RETURNING id, created_at;
                    """).format(store_name=sql.Identifier(store_name))
                    
                    cursor.execute(query, (Json(json_string),))
                
                #end if
                
                result                = cursor.fetchone()
                record_id, created_at = result
                
                self.mylogger.debug('PostgreSQL single record inserted into {store_name} with ID: {record_id}'.format(
                    store_name  = store_name,
                    record_id   = record_id
                ))
                return result
            #end with    
                
        except PostgreSQLError as err:
            self.mylogger.error('PostgreSQL single insert error in {store_name}: {err}'.format(
                store_name = store_name,
                err        = err
            ))
            raise DatabaseOperationError(f"PostgreSQL single insert failed: {err}")
    
        except Exception as err:
            self.mylogger.error('PostgreSQL single insert error in {store_name}: {err}'.format(
                store_name    = store_name,
                err           = err
            ))            
            
            raise DatabaseOperationError(f"PostgreSQL single insert failed: {err}")
        #end try
    #end insert_single
        
    
    def insert_multiple(self, 
                        data:               List[Dict[str, Any]], 
                        store_name:         str = "families", 
                        extract_unique_id:  bool = False, 
                        **kwargs) -> None:
        
        """
        Insert multiple JSON documents into PostgreSQL table
        
        Args:
            data:               List of dictionaries to insert
            store_name:         Name of the table
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
                        INSERT INTO {store_name} (uniqueId, data)
                        SELECT (jsonb_array_elements(%s)->>'uniqueId'), jsonb_array_elements(%s)
                        ON CONFLICT (uniqueId) DO NOTHING;
                    """).format(store_name=sql.Identifier(store_name))
                    
                    cursor.execute(query, (json_array_string, json_array_string))
                    
                else:
                    query = sql.SQL("""
                        INSERT INTO {store_name} (data) 
                        SELECT jsonb_array_elements(%s); 
                    """).format(store_name=sql.Identifier(store_name))
                    
                    cursor.execute(query, (json_array_string,))
                
                #end if
                
                self.mylogger.debug('PostgreSQL multiple record inserted into {store_name}'.format(
                    store_name  = store_name
                ))
            #end with
        except PostgreSQLError as err:
            self.mylogger.error('PostgreSQL multiple insert error in {store_name}: {err}'.format(
                store_name = store_name,
                err        = err
            ))
            raise DatabaseOperationError(f"PostgreSQL multiple insert failed: {err}")
        
        except Exception as err:
            self.mylogger.error('PostgreSQL multiple insert error in {store_name}: {err}'.format(
                store_name    = store_name,
                err           = err
            ))  
            
            raise DatabaseOperationError(f"PostgreSQL multiple insert failed: {err}")
        #end with
    #end insert_multiple
    
    
    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs) -> Any:
        
        """Universal insert method that routes to single or multiple insert"""
        
        if isinstance(data, list):
            if len(data) > 1:
                return self.insert_multiple(data, **kwargs)
            
            elif len(data) == 1:
                return self.insert_single(data[0], **kwargs)
            
            else:
                return []
        else:
            return self.insert_single(data, **kwargs)
        #end if
    #end insert
    
    
    def insert_flattened(self, 
                         data:          Dict[str, Any], 
                         store_name:    str = "documents", 
                         **kwargs) -> Any:
        
        """
        Insert flattened JSON data into PostgreSQL table columns
        
        Args:
            data: Dictionary containing the data to flatten
            store_name: Name of the table
            
        Returns:
            Result of the insert operation
        """
        
        try:
            with self.get_cursor() as cursor:
                # Build dynamic SQL based on data keys
                columns = ', '.join(data.keys())
                placeholders = ', '.join([f'%({key})s' for key in data.keys()])
                
                sql_query = f"INSERT INTO {store_name} ({columns}) VALUES ({placeholders}) RETURNING id;"
                
                cursor.execute(sql_query, data)
                result = cursor.fetchone()
                                
                self.mylogger.debug('PostgreSQL flattened record inserted into {store_name} with ID: {result[0]}'.format(
                    store_name       = store_name,
                    result           = result[0]
                ))
                
                return result
            #end with
        except PostgreSQLError as err:
            self.mylogger.error('PostgreSQL multiple insert error in {store_name}: {err}'.format(
                store_name = store_name,
                err        = err
            ))
            raise DatabaseOperationError(f"PostgreSQL multiple insert failed: {err}")
        
        except Exception as err:
            self.mylogger.error('PostgreSQL flattened insert error in {store_name}: {err}'.format(
                store_name    = store_name,
                err           = err
            ))
            
            raise DatabaseOperationError(f"PostgreSQL flattened insert failed: {err}")
        #end try
    #end insert_flattened
    
    
    def health_check(self) -> bool:

        """Perform PostgreSQL health check"""

        try:
            if self.connection and not self.connection.closed:
                with self.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
                #end with
            #end if
        except Exception as err:
            self.mylogger.error('PostgreSQL health check failed: {err}'.format(
                err = err
            ))
            self._is_connected = False
        
        return False
    #end health_check
    
    
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
            redis_params = self._build_redis_params()
            self.client  = redis.Redis(**redis_params)

            self.mylogger.debug('Redis Connection: {client}'.format(
                client = self.client
            ))
            
            # Test connection
            self.client.ping()
            self._is_connected = True
            self.mylogger.info('Redis connection established to: {host}:{port}/{db}'.format(
                host = self.config_params["REDIS_HOST"],
                port = self.config_params.get("REDIS_PORT", 6379), 
                db   = self.config_params.get("REDIS_DB",   0)
            ))
            
            return True
            
        except RedisConnectionError as err:
            self.mylogger.error('Redis connection failed: {host} {err}'.format(
                host = self.config_params["REDIS_HOST"],
                err  = err
            ))
            raise DatabaseConnectionError(f"Redis connection failed: {err}")
        
        except Exception as err:
            self.mylogger.error('Redis connection failed: {host}:{port} {err}'.format(
                host = self.config_params["REDIS_HOST"],
                port = self.config_params["REDIS_PORT"],
                err  = err
            ))
            
            raise DatabaseConnectionError(f"Redis connection failed: {err}")
        #end try
    #end connect
    
    
    def _build_redis_params(self) -> Dict[str, Any]:
        
        """Build Redis connection parameters"""
        
        redis_params = {
            'host':                     self.config_params["REDIS_HOST"],
            'port':                     self.config_params.get("REDIS_PORT", 6379),
            'db':                       self.config_params.get("REDIS_DB", 0),
            'decode_responses':         True,
            'socket_connect_timeout':   5,
            'socket_timeout':           5,
            'retry_on_timeout':         True
        }
        
        # Add password if provided
        if self.config_params.get("REDIS_PASSWORD"):
            redis_params['password'] = self.config_params["REDIS_PASSWORD"]
        
        # Handle SSL configuration
        if self.config_params.get("REDIS_SSL", False):
            redis_params.update(self._build_ssl_params())
        
        return redis_params
    #end _build_redis_params
    
    
    def _build_ssl_params(self) -> Dict[str, Any]:
        
        """Build SSL parameters for Redis connection"""
        
        ssl_params = {'ssl': True}
        
        ssl_verify = self.config_params.get("REDIS_SSL_VERIFY", True)
        if ssl_verify:
            ssl_params['ssl_cert_reqs'] = 'required'
            
            # Handle certificate files
            for cert_type, param_key, ssl_key in [
                ('CA', 'REDIS_SSL_CA_CERT', 'ssl_ca_certs'),
                ('client', 'REDIS_SSL_CLIENT_CERT', 'ssl_certfile'),
                ('key', 'REDIS_SSL_CLIENT_KEY', 'ssl_keyfile')
            ]:
                cert_path = self.config_params.get(param_key)
                if cert_path:
                    if not os.path.exists(cert_path):
                        raise FileNotFoundError(f"{cert_type} certificate file not found: {cert_path}")
                    
                    ssl_params[ssl_key] = cert_path
        else:
            ssl_params['ssl_cert_reqs'] = None
            self.mylogger.warning('SSL certificate verification is DISABLED - not recommended for production!')
        
        return ssl_params
    #end _build_ssl_params
    
    
    def _generate_key(self, store_name: str, key_field: str, data: Dict[str, Any]) -> str:
        
        """
        Generate Redis key with collection prefix
        
        Args:
            store_name: Collection name to use as prefix
            key_field:       Field name to extract from data as key suffix
            data:            Dictionary containing the data
            
        Returns:
            Formatted Redis key: store_name:field_value
        """
        
        if key_field not in data:
            raise ValueError(f"Key field '{key_field}' not found in data")
        
        key_value = data[key_field]
        return f"{store_name}:{key_value}"
    #end _generate_key
    
    
    def insert_single(self, 
                      data:             Dict[str, Any], 
                      store_name:  str, 
                      key_field:        str, 
                      ttl:              Optional[int] = None, 
                      **kwargs) -> str:
        
        """
        Insert single JSON document into Redis
        
        Args:
            data:               Dictionary containing the JSON data
            store_name:    Collection name to use as key prefix
            key_field:          Field name to use for key generation
            ttl:                Time to live in seconds (optional)
            
        Returns:
            Generated Redis key
        """
        
        try:
            if not self.client:
                raise DatabaseConnectionError("Redis not connected")
            
            #end if
            
            redis_key   = self._generate_key(store_name, key_field, data)
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
            
        except RedisError as err:
            self.mylogger.error('Redis single insert error for {store_name}: {err}'.format(
                store_name = store_name,
                err             = err
            ))
            raise DatabaseOperationError(f"Redis single insert failed: {err}")
        
        except Exception as err:
            self.mylogger.error('Redis single insert error for {store_name}: {err}'.format(
                store_name = store_name,
                err             = err
            ))
            
            raise DatabaseOperationError(f"Redis single insert failed: {err}")
        #end try
    #end insert_single
    
    
    def insert_multiple(self, 
                        data:               List[Dict[str, Any]], 
                        store_name:         str,
                        key_field:          str, 
                        ttl:                Optional[int] = None, 
                        **kwargs) -> List[str]:
        
        """
        Insert multiple JSON documents into Redis using pipeline
        
        Args:
            data:           List of dictionaries to insert
            store_name:     store <like table name in the single Redis DB, as paert of the key> name to use as key prefix  
            key_field:      Field name to use for key generation
            ttl:            Time to live in seconds (optional)
            
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
                redis_key   = self._generate_key(store_name, key_field, item)
                json_string = json.dumps(item)
                
                if ttl:
                    pipe.setex(redis_key, ttl, json_string)
            
                else:
                    pipe.set(redis_key, json_string)
                
                #end if    
                redis_keys.append(redis_key)
            
            #end for
            
            pipe.execute()
            
            self.mylogger.debug('Redis inserted {count} documents into {store_name}'.format(
                count           = len(data),
                store_name = store_name
            ))
            
            return redis_keys
            
        except RedisError as err:
            self.mylogger.error('Redis multiple insert error for {store_name}: {err}'.format(
                store_name = store_name,
                err             = err
            ))
            raise DatabaseOperationError(f"Redis multiple insert failed: {err}")
        
        except Exception as err:
            self.mylogger.error('Redis multiple insert error for {store_name}: {err}'.format(
                store_name = store_name,
                err             = err
            ))
            
            raise DatabaseOperationError(f"Redis multiple insert failed: {err}")
        #end try
    #end insert_multiple
    
    
    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs) -> Any:
        
        """Universal insert method that routes to single or multiple insert"""
        
        if isinstance(data, list):
            if len(data) > 1:
                return self.insert_multiple(data, **kwargs)
            
            elif len(data) == 1:
                return self.insert_single(data[0], **kwargs)
            
            else:
                return []
        else:
            return self.insert_single(data, **kwargs)
        #end if
    #end insert
    
    
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
            
        except RedisError as err:
            self.mylogger.error('Redis get error for key {redis_key}: {err}'.format(
                redis_key = redis_key,
                err       = err
            ))
            raise DatabaseOperationError(f"Redis get failed: {err}")
        
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
            
        except RedisError as err:
            self.mylogger.error('Redis pattern get error for {pattern}: {err}'.format(
                pattern = pattern,
                err     = err
            ))
            raise DatabaseOperationError(f"Redis pattern get failed: {err}")
        
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
            
        except RedisError as err:
            self.mylogger.error('Redis delete error for key {redis_key}: {err}'.format(
                redis_key = redis_key,
                err       = err
            ))
            raise DatabaseOperationError(f"Redis delete failed: {err}")
        
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


class KafkaConnection(DatabaseConnection):
    
    """
    Kafka Producer Manager with proper error handling and logging
    """

    def __init__(self, 
                 config_params: Dict[str, Any], 
                 mylogger):
        
        super().__init__(config_params, mylogger)
        
        self.max_retries    = config_params["MAXRETRIES"]    # Max retries for connection/production
        self.initial_delay  = config_params["DELAY"]         # Initial delay for exponential backoff
        self.producer       = None
        
    #end __init__
    
    
    def _build_kafka_config(self) -> Dict[str, Any]:
        
        """Builds Kafka producer configuration"""
        
        if self.config_params["SECURITY_PROTOCOL"] != "PLAINTEXT" and self.config_params["SECURITY_PROTOCOL"] != "":
            conf = {
                'bootstrap.servers':    self.config_params["BOOTSTRAP_SERVERS"],
                'sasl.mechanism':       self.config_params["SASL_MECHANISMS"],
                'security.protocol':    self.config_params["SECURITY_PROTOCOL"],
                'sasl.username':        self.config_params["SASL_USERNAME"],
                'sasl.password':        self.config_params["SASL_PASSWORD"],
                'client.id':            socket.gethostname(),
                'error_cb':             error_cb,
            }
        else:
            conf = {
                "bootstrap.servers":    self.config_params["BOOTSTRAP_SERVERS"],
                "security.protocol":    self.config_params["SECURITY_PROTOCOL"],
                "sasl.mechanism":       self.config_params["SASL_MECHANISMS"],
                'client.id':            socket.gethostname(),
                'error_cb':             error_cb,
            }        
        # end
        
                
        # if self.config_params.get("SCHEMAREGISTRY_SERVERS"):
        #     conf['schema.registry.url'] = self.config_params["SCHEMAREGISTRY_SERVERS"]
            
            self.mylogger.debug("Kafka Connect Detail ({conn})...".format(
                conn = conf
            ))
            return conf
    #end _build_kafka_config
    
    
    def _reconnect_with_retry(self) -> bool:
        
        """
        Attempts to reconnect to Kafka with exponential backoff.
        Retries up to self.max_retries times.
        """
        
        delay = self.initial_delay
        for i in range(self.max_retries):
            self.mylogger.info("Attempting Kafka connection ({max_retries})...".format(
                max_retries = (i+1/self.max_retries)
            ))
            try:
                conf                = self._build_kafka_config()
                self.producer       = Producer(conf)
                self._is_connected  = True
                self.mylogger.info("Kafka producer connection established to: {bootstrap}".format(
                    bootstrap = self.config_params['BOOTSTRAP_SERVERS']
                ))
                
                return True
            except (KafkaException, Exception) as err:
                self.mylogger.error("Kafka connection failed during retry: {err}. Retrying in {delay} seconds...".format(
                    err   = err,
                    delay = delay
                ))
                self.producer       = None      # Ensure producer is None on failure
                self._is_connected  = False
                time.sleep(delay)
                delay *= 2                      # Exponential backoff
                
            #end try
            
        self.mylogger.error("Failed to establish Kafka connection after {max_retries} retries.".format(
            max_retries = self.max_retries
        ))
        sys.exit(1)
        return False
    #end _reconnect_with_retry
    
    
    def connect(self) -> bool:

        """Establish Kafka producer connection, with retries."""

        return self._reconnect_with_retry()
    #end connect
    
    
    def disconnect(self):
        
        """Close Kafka producer connection and flush messages"""
        
        if self.producer:
            self.mylogger.debug('Final Flush of Kafka producer messages...')
            self.producer.flush(timeout=10)
            self.mylogger.debug('Kafka producer disconnected.')
        #end if
    #end disconnect
    
    
    def health_check(self) -> bool:
        
        """
        Perform Kafka health check.
        A simple poll can indicate if the producer is functional.
        """
        
        try:
            if self.producer:
                self.producer.poll(timeout=0.1)
                return True
            
            return False
            
        except Exception as err:
            self.mylogger.error('Kafka health check failed: {err}'.format(
                err = err
            ))
            self._is_connected = False
            return False
        #end try
    #end health_check
    
    
    def _delivery_report(self, err, msg):
        
        """Callback function for Kafka message delivery reports"""
        
        if err is not None:
            self.mylogger.error('Failed to Produce: Topic: {topic}, Err: {err}'.format(
                topic = msg.topic(),
                err   = err
            ))
        else:
            self.mylogger.debug('Produced to: Topic: {topic}, Partition: [{partition}], @ Offset: {offset}'.format(
                topic     = msg.topic(),
                partition = msg.partition(),
                offset    = msg.offset()
            ))
    #end _delivery_report
    
    
    def insert_single(self,
                      data:         Dict[str, Any],
                      store_name:   str,
                      key:          Optional[str] = None,
                      **kwargs) -> None:

        """
        Produce a single message to Kafka topic. Retries connection on failure.
        Flushes after successful production.
        
        Args:
            data:           Dictionary containing the message payload.
            store_name:     The name of the Kafka topic.
            key:            The field in `data` to use as the message key.
        """
        
        retries = 0
        while retries <= self.max_retries:
            try:
                if not self.producer:
                    if not self._reconnect_with_retry():
                        raise DatabaseConnectionError("Kafka producer not connected after retries.")

                payload_value = json.dumps(data).encode('utf-8')
                message_key   = None

                if key and key in data:
                    message_key = str(data[key]).encode('utf-8')

                self.producer.produce(topic     = store_name,
                                     value      = payload_value,
                                     key        = message_key,
                                     callback   = self._delivery_report)

                # Poll for delivery reports immediately after producing
                self.producer.poll(0)
                
                self.mylogger.debug('Kafka single message produced and polled for: {store_name}'.format(
                    store_name = store_name
                ))

                # Flush after single insert
                self.producer.flush(timeout=1)
                self.mylogger.info("Kafka producer flushed after single insert to topic: {store_name}".format(
                    store_name = store_name
                ))
                return # Success, exit loop

            except (KafkaException, DatabaseConnectionError) as err:
                self.mylogger.error("Kafka single insert failed on attempt {retries}: {err}".format(
                    retries = (retries+1/self.max_retries+1),
                    err     = err
                ))
                
                retries += 1
                
                if retries <= self.max_retries:
                    self.mylogger.info("Retrying Kafka single insert in {delay} seconds...".format(
                        delay = self.initial_delay * (2**(retries-1))
                    ))
                    
                    time.sleep(self.initial_delay * (2**(retries-1)))
                    # Attempt to reconnect before next retry
                    self._reconnect_with_retry()
                else:
                    raise DatabaseOperationError(f"Kafka single insert failed after {self.max_retries+1} attempts: {err}")
                
            except Exception as err:
                self.mylogger.error("An unexpected error occurred during Kafka single insert: {err}".format(
                    err = err
                ))
                raise DatabaseOperationError(f"Kafka single insert failed: {err}")
    #end insert_single


    def insert_multiple(self,
                        data:       List[Dict[str, Any]],
                        store_name: str,
                        key:        Optional[str] = None,
                        **kwargs) -> None:

        """
        Produce multiple messages to Kafka topic. Retries connection on failure.
        Flushes after all messages in the batch are produced.
        
        Args:
            data:           A list of dictionaries to insert.
            store_name:     The name of the Kafka topic.
            key:            The field in each `data` dict to use as the message key.
        """
        
        if not data:
            return

        retries = 0
        while retries <= self.max_retries:
            try:
                if not self.producer:
                    if not self._reconnect_with_retry():
                        raise DatabaseConnectionError("Kafka producer not connected after retries.")

                for record in data:
                    payload_value = json.dumps(record).encode('utf-8')
                    message_key   = None

                    if key and key in record:
                        message_key = str(record[key]).encode('utf-8')

                    self.producer.produce(topic     = store_name,
                                         value      = payload_value,
                                         key        = message_key,
                                         callback   = self._delivery_report)

                    # Poll frequently to allow delivery reports to be called
                    self.producer.poll(0)

                self.mylogger.debug('Kafka {count} messages produced to: {store_name}'.format(
                    count       = len(data),
                    store_name  = store_name
                ))

                # Flush after multiple inserts
                self.producer.flush(timeout=5) # Longer timeout for a batch flush
                
                self.mylogger.debug("Kafka producer flushed after multiple inserts to topic: {store_name}".format(
                    store_name = store_name
                ))
                return # Success, exit loop

            except (KafkaException, DatabaseConnectionError) as err:
                self.mylogger.error("Kafka multiple insert failed on attempt {attempt}: {err}".format(
                    attempt = (retries+1/self.max_retries+1),
                    err     = err
                ))
                
                retries += 1
                if retries <= self.max_retries:
                    self.mylogger.debug("Retrying Kafka multiple insert in {delay} seconds...".format(
                        delay = self.initial_delay * (2**(retries-1))
                    ))
                    time.sleep(self.initial_delay * (2**(retries-1)))
                    # Attempt to reconnect before next retry
                    self._reconnect_with_retry()
                else:
                    raise DatabaseOperationError("Kafka multiple insert failed after {retries} attempts: {err}".format(
                        retries = self.max_retries+1,
                        err     = err
                    ))
            except Exception as err:
                self.mylogger.error("An unexpected error occurred during Kafka multiple insert: {err}".format(
                    err = err
                ))
                raise DatabaseOperationError(f"Kafka multiple insert failed: {err}")
    #end insert_multiple
    
            
    def insert(self, data: Union[Dict[str, Any], List[Dict[str, Any]]], **kwargs) -> None:

        """
        Universal insert method that routes to single or multiple insert.
        Flushing is handled within insert_single/insert_multiple methods.
        """
    
        if isinstance(data, list):
            self.insert_multiple(data, **kwargs)
            
        else:
            self.insert_single(data, **kwargs)
    
    #end insert
#end KafkaConnection


class DatabaseManager:
    
    """Factory class for managing different database connections"""
    
    @staticmethod
    def create_connection(db_type:       str, 
                          config_params: Dict[str, Any], 
                          mylogger) -> DatabaseConnection:
        
        """
        Factory method to create appropriate database connection
        
        Args:
            db_type:        Type of database ('mongodb' or 'postgresql')
            config_params:  Configuration parameters
            mylogger:       mylogger instance
            
        Returns:
            DatabaseConnection instance
        """

        if db_type.lower() == 'mongodb':
            conn =  MongoDBConnection(config_params,    mylogger)
        
        elif db_type.lower() == 'postgresql':
            conn =  PostgreSQLConnection(config_params, mylogger)
        
        elif db_type.lower() == 'redis':
            conn =  RedisConnection(config_params,      mylogger)
        
        elif db_type.lower() == 'kafka':
            conn =  KafkaConnection(config_params,      mylogger)
    
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        #end if
                
        return conn
    #end create_connection
#end DatabaseManager


#############################################################
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


# Legacy functions for backward compatibility
def error_cb(err, logger):
    
    """Legacy error callback function"""

    logger.error("Client error: {err}".format(
        err = err
    ))
    
    if err.code() == KafkaError._ALL_BROKERS_DOWN or err.code() == KafkaError._AUTHENTICATION:
        raise KafkaException(err)
#end error_cb


def create_kafka_producer(config_params, logger):
    
    """Legacy function to create Kafka producer"""
    
    manager = KafkaConnection(config_params, logger)
    
    return manager.create_producer()
#end create_kafka_producer


def acked(err, msg, logger):
    
    """Legacy delivery callback function"""
    
    if err is not None:
        logger.error('Failed to Produce: Topic: {topic}, Err: {err}'.format(
            topic = msg.topic(), 
            err   = err
        ))
    else:
        logger.debug('Produced to: Topic: {msg.topic()} Part: [{partition}] @ Offset: {offset}'.format(
            topic     = msg.topic(), 
            partition = msg.partition(),
            offset    = msg.offset()
        ))
#end acked
