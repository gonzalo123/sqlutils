<?php

include __DIR__ . '/../vendor/autoload.php';

use Doctrine\DBAL\DriverManager;
use G\SqlUtils\Upsert;

$connectionParams = [
    'dbname'   => 'gonzalo',
    'user'     => 'username',
    'password' => 'password',
    'host'     => 'localhost',
    'driver'   => 'pdo_pgsql',
];

$dbh = DriverManager::getConnection($connectionParams);
$dbh->transactional(function ($conn) {
    Upsert::createFromDBAL($conn)->exec('public.tbupsertexample', [
        'KEY1' => 'key1',
        'KEY2' => 'key2',
        'KEY3' => 'key3',
        'KEY4' => 'key4',
    ], [
        'VALUE1' => 'value1',
        'VALUE2' => 'value2',
        'VALUE3' => 'value3',
        'VALUE4' => null,
        'VALUE5' => 'value5',
    ]);
});
