<?php

include __DIR__ . '/../vendor/autoload.php';

use G\SqlUtils\Upsert;

$conn = new PDO('pgsql:dbname=gonzalo;host=localhost', 'username', 'password');
$conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$conn->beginTransaction();
try {
    Upsert::createFromPDO($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
        'KEY1' => 'key1',
        'KEY2' => 'key2',
        'KEY3' => 'key3',
        'KEY4' => 'key4',
    ], [
        'VALUE1' => 'value1',
        'VALUE2' => 'value2',
        'VALUE3' => 'value3',
        'VALUE4' => 'value4',
        'VALUE5' => 'value5',
    ]);
    $conn->commit();
} catch (Exception $e) {
    $conn->rollback();
    throw $e;
}