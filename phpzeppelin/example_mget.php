<?php

$zp_mget = new Zeppelin("127.0.0.1:9801", "example_mget_test");

$times = 5;
while ($times--) {
  $ret = $zp_mget->Set("example_mget_key_".$times, "example_mget_value_".$times);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
}

$times = 5;
$keys = array();
while ($times--) {
  array_push($keys, "example_mget_key_".$times);
}

var_dump($keys);

$ret = $zp_mget->Mget($keys);
if ($ret == false) {
  echo "Mget Error". PHP_EOL;
}
var_dump($ret);
