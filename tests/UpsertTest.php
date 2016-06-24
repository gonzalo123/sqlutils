<?php
use Doctrine\DBAL\DriverManager;
use G\SqlUtils\Upsert;

class UpsertTest extends \PHPUnit_Framework_TestCase
{
    public function testPDO()
    {
        $conn = new PDO('pgsql:dbname=gonzalo;host=localhost', 'username', 'password');
        $conn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $conn->exec(file_get_contents(__DIR__ . '/fixtures/createTable.sql'));

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();

        $this->assertEquals(0, count($result));

        $conn->beginTransaction();
        try {
            Upsert::createFromPDO($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
                'key1' => 'key1',
                'key2' => 'key2',
                'key3' => 'key3',
                'key4' => 'key4',
            ], [
                'value1' => 'value1',
                'value2' => 'value2',
                'value3' => 'value3',
                'value4' => null,
                'value5' => 'value5',
            ]);
            $conn->commit();
        } catch (Exception $e) {
            $conn->rollback();
            throw $e;
        }

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();
        $this->assertEquals('value1', $result[0]['value1']);
        $this->assertEquals('value2', $result[0]['value2']);
        $this->assertEquals('value3', $result[0]['value3']);
        $this->assertEquals(null, $result[0]['value4']);
        $this->assertEquals('value5', $result[0]['value5']);

        $conn->beginTransaction();
        try {
            Upsert::createFromPDO($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
                'key1' => 'key1',
                'key2' => 'key2',
                'key3' => 'key3',
                'key4' => 'key4',
            ], [
                'value1' => 'value1',
                'value2' => 'value2',
                'value3' => 'value3',
                'value4' => 'value4',
                'value5' => 'value5',
            ]);
            $conn->commit();
        } catch (Exception $e) {
            $conn->rollback();
            throw $e;
        }

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();
        $this->assertEquals('value1', $result[0]['value1']);
        $this->assertEquals('value2', $result[0]['value2']);
        $this->assertEquals('value3', $result[0]['value3']);
        $this->assertEquals('value4', $result[0]['value4']);
        $this->assertEquals('value5', $result[0]['value5']);

        $conn->exec(file_get_contents(__DIR__ . '/fixtures/dropTable.sql'));
    }

    public function testDBAL()
    {
        $connectionParams = [
            'dbname'   => 'gonzalo',
            'user'     => 'username',
            'password' => 'password',
            'host'     => 'localhost',
            'driver'   => 'pdo_pgsql',
        ];

        $conn = DriverManager::getConnection($connectionParams);

        $conn->exec(file_get_contents(__DIR__ . '/fixtures/createTable.sql'));

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();

        $this->assertEquals(0, count($result));

        $conn->transactional(function ($conn) {
            Upsert::createFromDBAL($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
                'key1' => 'key1',
                'key2' => 'key2',
                'key3' => 'key3',
                'key4' => 'key4',
            ], [
                'value1' => 'value1',
                'value2' => 'value2',
                'value3' => 'value3',
                'value4' => null,
                'value5' => 'value5',
            ]);
        });

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();
        $this->assertEquals('value1', $result[0]['value1']);
        $this->assertEquals('value2', $result[0]['value2']);
        $this->assertEquals('value3', $result[0]['value3']);
        $this->assertEquals(null, $result[0]['value4']);
        $this->assertEquals('value5', $result[0]['value5']);

        $conn->transactional(function ($conn) {
            Upsert::createFromDBAL($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
                'key1' => 'key1',
                'key2' => 'key2',
                'key3' => 'key3',
                'key4' => 'key4',
            ], [
                'value1' => 'value1',
                'value2' => 'value2',
                'value3' => 'value3',
                'value4' => 'value4',
                'value5' => 'value5',
            ]);
        });

        $sth    = $conn->query("SELECT * FROM PUBLIC.TBUPSERTEXAMPLE");
        $result = $sth->fetchAll();
        $this->assertEquals('value1', $result[0]['value1']);
        $this->assertEquals('value2', $result[0]['value2']);
        $this->assertEquals('value3', $result[0]['value3']);
        $this->assertEquals('value4', $result[0]['value4']);
        $this->assertEquals('value5', $result[0]['value5']);

        $conn->exec(file_get_contents(__DIR__ . '/fixtures/dropTable.sql'));
    }
}