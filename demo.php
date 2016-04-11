<?php
include __DIR__.'/src/MysqlAerospike/Connection.php';
include __DIR__.'/src/MysqlAerospike/Mysql.php';

$config = array(
    'connection_config' => array(
        'hosts' => array(
            array('addr' => '127.0.0.1', 'port' => 3000),
        ),
        'user' => '',
        'pass' => '',
    ),
    'persistent' => false,
    'connection_options' => class_exists('Aerospike', false) ? array(
        Aerospike::OPT_CONNECT_TIMEOUT => 500,
        Aerospike::OPT_READ_TIMEOUT => 500,
        Aerospike::OPT_WRITE_TIMEOUT => 500,
        Aerospike::OPT_POLICY_KEY => Aerospike::POLICY_KEY_SEND,
    ) : array(),
    'namespace' => 'test',
    'indices' => array(
        'user' => array(
            'username' => array(
                'bin' => 'username',
                'type' => 'string_ci',
            ),
            'id' => array(
                'bin' => 'id',
                'type' => 'integer',
            ),
            'email' => array(
                'bin' => 'email',
                'type' => 'string_ci',
            ),
        ),
    ),
    'mysql' => array(
        'service' => 'localhost',
        'database' => 'test',
        'user' => 'root',
        'password' => '123456'
    )
);

/*
$mysql = new \xinson\MysqlAerospike\Mysql($config['mysql']['service'], $config['mysql']['database'],
$config['mysql']['user'], $config['mysql']['password']);
$mysql->query("CREATE TABLE `user` (
  `username` VARCHAR(16) NOT NULL,
  `email` VARCHAR(255) NULL,
  `password` VARCHAR(32) NOT NULL,
  `create_time` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP)");
$data = $mysql->insert('user',  array( 'username' => 'xinson', 'email' => '513730858@qq.com'));
$data = $mysql->update('user',  array('username' => 'xinson1'), array('id' => 1));
$data = $mysql->getOne('user', array(), array('id' => 1));
$data = $mysql->getAll('user', array(), array('id' => 1));
$data = $mysql->delete('user',  array('id' => 2));
*/
$Aerospike = new \xinson\MysqlAerospike\Connection($config);

/*
foreach ($config['indices'] as $table => $indexDef) {
    $Aerospike->createIndices($table, $indexDef);
}
*/
//$Aerospike->saveTableDefinition('user');
//$Aerospike->removeTableDefinition('user');
$Aerospike->saveMysqlTableToAerospike('user');


