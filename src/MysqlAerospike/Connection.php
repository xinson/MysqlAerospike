<?php
namespace Xinson\MysqlAerospike;

use Aerospike;

class Connection
{

    public $aerospike;
    public $namespace;
    public $config;
    public $tableDefinitions = array();
    private static $db;

    public function __construct($config)
    {
        $this->config = $config;
        if (!class_exists('Aerospike', false)) {
            exit(" PHP extension aerospike not installed! \n");
        }
        $this->namespace = $this->config['namespace'];
        $this->aerospike = new Aerospike($this->config['connection_config'], $this->config['persistent'],
            $this->config['connection_options']);
        if (!$this->aerospike->isConnected()) {
            exit("Aerospike failed to connect: " . $this->aerospike->error());
        }
    }

    public function fnGet(&$array, $key, $default = null)
    {
        if (($sub_key_pos = strpos($key, '/')) === false) {
            if (is_object($array)) {
                return property_exists($array, $key) ? $array->$key : $default;
            }
            return isset($array[$key]) ? $array[$key] : $default;
        } else {
            $first_key = substr($key, 0, $sub_key_pos);
            if (is_object($array)) {
                $tmp = property_exists($array, $first_key) ? $array->$first_key : null;
            } else {
                $tmp = isset($array[$first_key]) ? $array[$first_key] : null;
            }
            return $this->fnGet($tmp, substr($key, $sub_key_pos + 1), $default);
        }
    }

    public function array_filter_key(array $array, $callback)
    {
        if (defined('ARRAY_FILTER_USE_KEY')) {
            return array_filter($array, $callback, ARRAY_FILTER_USE_KEY);
        } else {
            $result = array();
            foreach ($array as $k => $v) {
                call_user_func($callback, $k) and $result[$k] = $v;
            }
            return $result;
        }
    }

    public function getDb()
    {
        if (self::$db == null) {
            self::$db = new Mysql($this->config['mysql']['service'], $this->config['mysql']['database'],
                $this->config['mysql']['user'], $this->config['mysql']['password'], $port = 3306);
        }
        return self::$db;
    }

    /**
     * @param $table
     * @param array $parameter array('name' => 'jack', 'email' => 'someone@email.com')
     * @return bool
     */
    public function insert($table, array $parameter)
    {
        $set = $table;
        $options = array();
        $data = $this->prepareInsertData($set, $parameter);
        $pkValue = $this->fnGet($data, $pk = $this->getTableDefinitions($set, 'pk'));
        $key = $this->aerospike->initKey($this->namespace, $set, $pkValue);
        $status = $this->aerospike->put($key, $data, 0, $options);
        if ($status == Aerospike::OK) {
            //$this->syncToMysql($set, $bindings, $pk, $pkValue);
            return true;
        }
        return false;
    }

    /**
     * @param $table
     * @param array $parameter array('name' => 'jack', 'email' => 'someone@email.com')
     * @param array $condition array('id' => 2, 'name' => 'jack')
     * @return bool
     */
    public function update($table, array $parameter, array $condition)
    {
        $pk = $this->getTableDefinitions($table, 'pk');
        isset($parameter[$pk]) or $parameter[$pk] = reset($parameter);
        $set = $table;
        $options = array();
        $data = $this->prepareInsertData($set, $parameter);
        $pkValue = $this->fnGet($data, $pk = $this->getTableDefinitions($set, 'pk'));
        $key = $this->aerospike->initKey($this->namespace, $set, $pkValue);
        $status = $this->aerospike->put($key, $data, 0, $options);
        if ($status == Aerospike::OK) {
            //$this->syncToMysql($set, $bindings, $pk, $pkValue);
            return true;
        }
        return false;
    }

    /**
     * @param $table
     * @param array $condition array('id' => 2, 'name' => 'jack')
     * @return bool
     */
    public function delete($table, array $condition)
    {
        $this->aerospike->query();
        //$id === null and $id = $this->fnGet($condition, 'val');
        //if (!$id) return false;
        $set = $table;
        $options = array();
        $key = $this->aerospike->initKey($this->namespace, $set, $id);
        $status = $this->aerospike->remove($key, $options);
        return $status == Aerospike::OK;
    }

    /**
     * @param $table
     * @param array $field
     * @param array $condition
     * @return array
     */
    public function getOne($table, array $field = array(), array $condition = array())
    {
        $result = array();
        $set = $table;
        $where = $condition;
        $select = $field;
        $options = array();
        $this->aerospike->query($this->namespace, $set, $where,
            function ($record) use (&$result) {
                if ($bins = $this->fnGet($record, 'bins')) {
                    $data = array_merge($bins, $this->fnGet($bins, '_long_keys'));
                    unset($data['_long_keys']);
                    $data = $this->array_filter_key($data, function ($k) {
                        return $k{0} != '_';
                    });
                    $result[] = $data;
                }
            },
            $select, $options);
        return $result;
    }


    public function unquoteField($field)
    {
        ($pos = strpos($field, '.')) === false or $field = substr($field, $pos + 1);
        return str_replace('`', '', $field);
    }

    protected function prepareInsertData($table, $data)
    {
        // Remove undefined fields
        $fields = (array)$this->getTableDefinitions($table, 'fields');
        $data = array_intersect_key((array)$data, $fields);
        // Prepare case insensitive indices
        $indices = $this->getTableDefinitions($table, 'indices');
        foreach ($indices as $index) {
            if (!isset($data[$bin = $this->fnGet($index, 'bin')])) {
                continue;
            }
            if (($indexType = $this->fnGet($index, 'type')) == 'string_ci') {
                $data[$bin] = mb_strtolower($data[$bin]);
            } else {
                if ($indexType == 'integer') {
                    $data[$bin] = (int)$data[$bin];
                }
            }
        }
        $result = array();
        $longKeyData = array();
        foreach ($data as $k => $v) {
            if (is_object($v)) {
                continue;
            }
            if (strlen($k) > 14) {
                $longKeyData[$k] = $v;
            } else {
                $result[$k] = $v;
            }
        }
        $result['_long_keys'] = $longKeyData;
        return $result;
    }


    public function getTableDefinitions($table, $definitionItem = null)
    {
        if (!isset($this->tableDefinitions[$table])) {
            $key = $this->aerospike->initKey($this->namespace, 'tables', $table);
            $status = $this->aerospike->get($key, $record);
            if ($status == Aerospike::ERR_RECORD_NOT_FOUND) {
                $record = false;
            } else {
                if ($status != Aerospike::OK) {
                    throw new ConnectionException('Aerospike failed to get: ' . $this->aerospike->error(),
                        $this->aerospike->errorno());
                }
            }
            $definition = $this->fnGet($record, 'bins');
            //$definition['indices'] = $this->getTableIndices($table);
            $this->tableDefinitions[$table] = $definition;
        }
        return $this->fnGet($this->tableDefinitions, $table . ($definitionItem ? '/' . $definitionItem : ''));
    }


    /**
     * @param $table
     */
    public function saveTableDefinition($table)
    {
        $db = $this->getDb();
        $tableStatus = $db->query(sprintf('SHOW TABLE STATUS LIKE "%s"', $table));
        $ai = $this->fnGet($tableStatus, 'Auto_increment');
        $columns = $db->query(sprintf('SHOW COLUMNS FROM %s', $table));
        $fields = array();
        $pkAutoIncrement = false;
        $primaryKey = '';
        foreach ($columns as $col) {
            $fields[$field = $this->fnGet($col, 'Field')] = array(
                'type' => $this->fnGet($col, 'Type'),
                'default' => $this->fnGet($col, 'Default'),
            );
            $this->fnGet($col, 'Extra') == 'auto_increment' and $pkAutoIncrement = true;
            $this->fnGet($col, 'Key') == 'PRI' and $primaryKey = $field;
        }
        $definition = array(
            'table' => $table,
            'pk' => $primaryKey,
            'is_ai' => $pkAutoIncrement,
            'ai' => (int)$ai,
            'fields' => $fields
        );
        $key = $this->aerospike->initKey($this->namespace, 'tables', $table);
        $this->aerospike->put($key, $definition, 0);

    }

    /**
     * @param $table
     * @return $this
     */
    public function removeTableDefinition($table)
    {
        $key = $this->aerospike->initKey($this->namespace, 'tables', $table);
        $this->aerospike->remove($key);
        return $this;
    }

    /**
     * @param null $table
     * @return $this
     */
    public function resetTableDefinitions($table = null)
    {
        if ($table === null) {
            $this->tableDefinitions = array();
        } else {
            unset($this->tableDefinitions, $table);
        }
        return $this;
    }


    /**
     * @param $table
     * @param array $indices
     */
    public function createIndices($table, array $indices)
    {
        $types = array(
            'string' => Aerospike::INDEX_TYPE_STRING,
            'string_ci' => Aerospike::INDEX_TYPE_STRING,
            'integer' => Aerospike::INDEX_TYPE_INTEGER
        );
        $caseInsensitiveBins = array();
        foreach ($indices as $index) {
            $bin = $this->fnGet($index, 'bin');
            $name = $table . '_' . $bin;
            $type = $this->fnGet($index, 'type');
            if ($type == 'string_ci') {
                $caseInsensitiveBins[$bin] = $bin;
            }
            $status = $this->aerospike->createIndex($this->namespace, $table, $bin,
                $this->fnGet($types, $type), $name);
            if ($status == Aerospike::ERR_INDEX_FOUND) {
                unset($caseInsensitiveBins[$bin]);
            }
        }
    }

    public function migrationSaveStartId($table, $startId)
    {
        $key = $this->aerospike->initKey($this->namespace, 'migration', $table);
        $this->aerospike->put($key, array('start_id' => $startId), 0);
        return $this;
    }

    public function migrationGetStartId($table)
    {
        $key = $this->aerospike->initKey($this->namespace, 'migration', $table);
        $this->aerospike->get($key, $result);
        return (int)$this->fnGet($result, 'bins/start_id', 0);
    }

    protected function getMigrationCount($table, $startId, $idField = 'id')
    {
        $mysql = $this->getDb();
        $rs = $mysql->getOne($table, array("COUNT(`{$idField}`) as `count`"), "`{$idField}` > {$startId}");
        $count = $this->fnGet($rs, 'count');
        return $count;
    }



}
