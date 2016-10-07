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
        $insertValues = [];
        $fields       = [];
        $keys         = [];
        $updateSet    = [];
        $updateWhere  = [];
        $params       = [];

        foreach ($key as $k => $v) {
            $fields[]       = $k;
            $insertValues[] = ":{$k}";
            $keys[]         = $k;
            $updateWhere[]  = "TBUPSERTEXAMPLE.{$k} = :{$k}";
            $params[$k]     = $v;
        }

        foreach ($values as $k => $v) {
            $fields[]       = $k;
            $insertValues[] = ":{$k}";
            $updateSet[]    = "$k = :{$k}";
            $params[$k]     = $v;
        }

        $fields       = implode(", ", $fields);
        $insertValues = implode(", ", $insertValues);
        $keys         = implode(", ", $keys);
        $updateSet    = implode(", ", $updateSet);
        $updateWhere  = implode(" and ", $updateWhere);

        $sql = "
            insert into {$table} ({$fields})
              values ({$insertValues})
            on conflict ({$keys})
            do update set 
              {$updateSet}
            where 
              {$updateWhere};
        ";

        $smtp = $this->connection->prepare($sql);
        $smtp->execute($params);
    }
}