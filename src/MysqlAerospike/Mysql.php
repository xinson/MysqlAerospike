<?php
namespace Xinson\MysqlAerospike;

use PDO;
/**
 * Class Mysql
 * @package xinson\MysqlAerospike
 */
class Mysql extends PDO
{
    protected $service;
    protected $database;
    protected $user;
    protected $password;
    protected $port;

    /**
     * Mysql constructor.
     * @param $service
     * @param $database
     * @param $user
     * @param $password
     * @param int $port
     */
    public function __construct($service, $database, $user, $password, $port = 3306)
    {
        $this->service = $service;
        $this->database = $database;
        $this->user = $user;
        $this->password = $password;
        $this->port = $port;

        parent::__construct("mysql:host=$this->service;port=$port;dbname=$this->database", $this->user,
            $this->password);
        $this->query("SET NAMES utf8");
    }

    /**
     * @param $table
     * @param array $field array('id', 'name')
     * @param $where "id = 2"
     * @return mixed
     */
    public function getOne($table, array $field = array(), $where)
    {
        if (empty($field)) {
            $fields = '*';
        } else {
            $fields = implode(',', $field);
        }
        $sqlwhere = '';
        if (!empty($where)) {
            $sqlwhere = " WHERE " . $where;
        }
        $query = $this->query("SELECT {$fields} FROM {$table} $sqlwhere");
        return $query->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * @param $table
     * @param array $field array('id', 'name')
     * @param $where "id = 2"
     * @return mixed
     */
    public function getAll($table, array $field = array(), $where)
    {
        if (empty($field)) {
            $fields = '*';
        } else {
            $fields = implode(',', $field);
        }
        $sqlwhere = '';
        if (!empty($where)) {
            $sqlwhere = " WHERE " . $where;
        }
        $query = $this->query("SELECT {$fields} FROM {$table} $sqlwhere");
        return $query->fetchAll(PDO::FETCH_ASSOC);
    }


    /**
     * @param $table
     * @param $where "id = 2"
     * @return bool
     */
    public function delete($table, $where)
    {
        if (empty($where)) {
            return false;
        }
        $sqlwhere = " WHERE " . $where;
        $res = $this->query("DELETE FROM {$table} {$sqlwhere}");
        if ($res) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param $table
     * @param $parameter array('name' => 'jack', 'email' => 'someone@email.com')
     * @return bool
     */
    public function insert($table, array $parameter)
    {
        if (empty($parameter)) {
            return false;
        }
        $arrkeys = implode(',', array_keys($parameter));
        $arrvalues = implode("','", array_values($parameter));
        $sql = "INSERT INTO {$table} ({$arrkeys}) VALUES('{$arrvalues}');";
        $rs = $this->query($sql);
        if ($rs) {
            return $this->lastInsertId();
        } else {
            return false;
        }
    }

    /**
     * @param $table
     * @param array $parameter array('name' => 'jack', 'email' => 'someone@email.com')
     * @param $where "id = 2"
     * @return bool
     */
    public function update($table, array $parameter, $where)
    {
        $parameters = '';
        foreach ($parameter as $k => $v) {
            $parameters[] = $k . "='" . $v . "'";
        }
        $sqlwhere = '';
        if (!empty($where)) {
            $sqlwhere = " WHERE " . $where;
        }
        $query = $this->query("UPDATE {$table} SET " . implode(',', $parameters) . $sqlwhere);
        if ($query) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * @param $table
     * @return bool
     */
    public function drop($table)
    {
        $sql = "DROP TABLE {$table}";
        $rs = $this->query($sql);
        if ($rs) {
            return true;
        } else {
            return false;
        }
    }

}
