<?php

namespace G\SqlUtils;

use Doctrine\DBAL\Connection;
use PDO;

class Upsert
{
    private $connection;

    public static function createFromDBAL(Connection $connection)
    {
        $upsert = new self();
        $upsert->setConnectionDBAL($connection);

        return $upsert;
    }

    public static function createFromPDO(PDO $connection)
    {
        $upsert = new self();
        $upsert->setConnectionPDO($connection);

        return $upsert;
    }

    public function setConnectionDBAL(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function setConnectionPDO(PDO $connection)
    {
        $this->connection = $connection;
    }

    public function exec($table, $key, $values)
    {
        $setArr          = [];
        $whereArr        = [];
        $insertFieldsArr = [];
        $selectFieldsArr = [];
        $params          = [];

        foreach ($values as $k => $v) {
            $setArr[]          = "{$k} = :{$k}";
            $insertFieldsArr[] = $k;
            $selectFieldsArr[] = ":{$k}";
            $params[$k]        = $v;
        }
        $set = implode(", ", $setArr);

        foreach ($key as $k => $v) {
            $whereArr[]        = "{$k} = :{$k}";
            $insertFieldsArr[] = $k;
            $selectFieldsArr[] = ":{$k}";
            $params[$k]        = $v;
        }
        $where        = implode(" AND ", $whereArr);
        $insertFields = implode(", ", $insertFieldsArr);
        $selectFields = implode(", ", $selectFieldsArr);

        $sql  = "
            WITH upsert AS (
                UPDATE {$table}
                SET
                    {$set}
            WHERE
                {$where}
                RETURNING *
            )
            INSERT INTO {$table} ({$insertFields})
            SELECT
                {$selectFields}
            WHERE
                NOT EXISTS (SELECT 1 FROM upsert);
        ";
        $smtp = $this->connection->prepare($sql);
        $smtp->execute($params);
    }
}