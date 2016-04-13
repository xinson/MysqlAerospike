<?php
include __DIR__.'/src/MysqlAerospike/Connection.php';
include __DIR__.'/src/MysqlAerospike/Mysql.php';
include __DIR__.'/src/MysqlAerospike/ConnectionException.php';

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
        'demo_user' => array(
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
$mysql->query("CREATE TABLE IF NOT EXISTS `demo_user` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `username` varchar(255) NOT NULL COMMENT '用户名称',
  `email` varchar(255) DEFAULT '' COMMENT '用户邮箱',
  PRIMARY KEY (`id`),
  KEY `username` (`username`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8 COMMENT='用户表' AUTO_INCREMENT=1");
//$data = $mysql->insert('demo_user',  array( 'username' => 'xinson', 'email' => '513730858@qq.com'));
//$data = $mysql->update('demo_user',  array('username' => 'xinson1'), "username = 'xinson'");
$data = $mysql->getOne('demo_user', array(), "id = 1");
//$data = $mysql->getAll('demo_user', array(), "id = 1");
//$data = $mysql->delete('demo_user',  "id = 1");
*/

$Aerospike = new \xinson\MysqlAerospike\Connection($config);
foreach ($config['indices'] as $table => $indexDef) {
    $Aerospike->createIndices($table, $indexDef);
}

//$Aerospike->saveTableDefinition('demo_user');
//$Aerospike->removeTableDefinition('user');
$Aerospike->saveMysqlTableToAerospike('demo_user');


