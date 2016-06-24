SqlUtils
===========

Sql utils for PostgreSQL

[![Build Status](https://travis-ci.org/gonzalo123/sqlutils.svg?branch=master)](https://travis-ci.org/gonzalo123/sqlutils)

## upsert
How to perform an UPDATE statement or an INSERT statement if row doesn't exits.

Imagine the following table

```sql
CREATE TABLE PUBLIC.TBUPSERTEXAMPLE
(
  KEY1 CHARACTER VARYING(10) NOT NULL,
  KEY2 CHARACTER VARYING(14) NOT NULL,
  KEY3 CHARACTER VARYING(14) NOT NULL,
  KEY4 CHARACTER VARYING(14) NOT NULL,

  VALUE1 CHARACTER VARYING(20),
  VALUE2 CHARACTER VARYING(20) NOT NULL,
  VALUE3 CHARACTER VARYING(100),
  VALUE4 CHARACTER VARYING(400),
  VALUE5 CHARACTER VARYING(20),

  CONSTRAINT TBUPSERTEXAMPLE_PKEY PRIMARY KEY (KEY1, KEY2, KEY3, KEY4)
)
```

We can perform an 'upsert' statement like this:

```sql
WITH upsert AS (
    UPDATE PUBLIC.TBUPSERTEXAMPLE
    SET
        VALUE1 = :VALUE1,
        VALUE2 = :VALUE2,
        VALUE3 = :VALUE3,
        VALUE4 = :VALUE4,
        VALUE5 = :VALUE5
    WHERE
        KEY1 = :KEY1 AND
        KEY2 = :KEY2 AND
        KEY2 = :KEY3 AND
        KEY3 = :KEY4
    RETURNING *
)
INSERT INTO PUBLIC.TBUPSERTEXAMPLE(KEY1, KEY2, KEY3, KEY4, VALUE1, VALUE2, VALUE3, VALUE4, VALUE5)
SELECT
    :KEY1, :KEY2, :KEY3, :KEY4, :VALUE1, :VALUE2, :VALUE3, :VALUE4, :VALUE5
WHERE
    NOT EXISTS (SELECT 1 FROM upsert);
```

### PDO usage example Example

```php
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
```

### DBAL usage example Example

```php
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
    Upsert::createFromDBAL($conn)->exec('PUBLIC.TBUPSERTEXAMPLE', [
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
```