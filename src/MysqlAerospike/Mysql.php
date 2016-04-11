<?php
namespace xinson\MysqlAerospike;
use PDO;

class Mysql extends PDO{

    public $service;
    public $database;
    public $user;
    public $password;
    public $port;

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

        parent::__construct("mysql:host=$this->service;port=$port;dbname=$this->database",$this->user,$this->password);
        $this->query("SET NAMES utf8");
    }

    /**
     * @param $table
     * @param array $field array('id', 'name')
     * @param array $condition array('id' => 2, 'name' => 'jack')
     * @return mixed
     */
    public function getOne($table, array $field = array(), array $condition = array())
    {
        if(empty($field)){
            $fields = '*';
        }else {
            $fields = implode(',', $field);
        }
        $where = '';
        if(!empty($condition)) {
            $conditions = array();
            foreach ($condition as $k => $v) {
                $conditions[] = $k . ' = :' . $k;
            }
            $where =  " WHERE ". implode(' AND ', $conditions);
        }
        $stmt = $this->prepare("SELECT {$fields} FROM {$table} $where");
        if(!empty($condition)) {
            foreach ($condition as $k => $v) {
                $stmt->bindValue(':' . $k, $v);
            }
        }
        $stmt->execute();
        return $stmt->fetch(PDO::FETCH_ASSOC);

    }

    /**
     * @param $table
     * @param array $field  array('id', 'name')
     * @param array $condition  array('id' => 2, 'name' => 'jack')
     * @return mixed
     */
    public function getAll($table, array $field = array(), array $condition = array())
    {
        if(empty($field)){
            $fields = '*';
        }else {
            $fields = implode(',', $field);
        }
        $where = '';
        if(!empty($condition)) {
            $conditions = array();
            foreach ($condition as $k => $v) {
                $conditions[] = $k . ' = :' . $k;
            }
            $where =  " WHERE ". implode(' AND ', $conditions);
        }
        $stmt = $this->prepare("SELECT {$fields} FROM {$table} $where");
        if(!empty($condition)) {
            foreach ($condition as $k => $v) {
                $stmt->bindValue(':' . $k, $v);
            }
        }
        $stmt->execute();
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }


    /**
     * @param $table
     * @param array $condition  array('id' => 2, 'name' => 'jack')
     * @return bool
     */
    public function delete($table, array $condition)
    {
        if(empty($condition)) return false;
        $conditions = array();
        foreach($condition as $k=>$v){
            $conditions[] = $k .'= :' .$k;
        }
        $where  =  " WHERE ". implode(' AND ', $conditions);
        $stmt = $this->prepare("DELETE FROM {$table} {$where}");
        foreach($condition as $k=>$v){
            $stmt->bindValue(':' . $k, $v);
        }
        if($stmt->execute()){
            return true;
        }else{
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
        if(empty($parameter)) return false;
        $arrkeys = implode(',', array_keys($parameter));
        $arrvalues = implode(',', array_values($parameter));
        $sql = "INSERT INTO {$table} ({$arrkeys}) VALUES({$arrvalues});";
        $rs = $this->query($sql);
        if($rs){
            $this->lastInsertId();
        }else{
            return false;
        }
    }

    /**
     * @param $table
     * @param array $parameter array('name' => 'jack', 'email' => 'someone@email.com')
     * @param array $condition  array('id' => 2, 'name' => 'jack')
     * @return bool
     */
    public function update($table, array $parameter, array $condition = array())
    {
        $parameters = '';
        foreach($parameter as $k=>$v)
        {
            $parameters[] = $k .' = '.$v;
        }
        $where = '';
        if(!empty($condition)) {
            $conditions = array();
            foreach ($condition as $k => $v) {
                $conditions[] = $k . ' = :' . $k;
            }
            $where =  " WHERE ". implode(' AND ', $conditions);
        }
        $stmt = $this->prepare("UPDATE {$table} SET " . implode(',',$parameters) . $where);
        if(!empty($condition)) {
            foreach ($condition as $k => $v) {
                $stmt->bindValue(':' . $k, $v);
            }
        }
        if($stmt->execute()){
            return true;
        }else{
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
        if($rs){
            return true;
        }else{
            return false;
        }
    }

}
