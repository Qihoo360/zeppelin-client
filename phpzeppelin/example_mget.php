<?php

$zp_mget = new Zeppelin("127.0.0.1", "9801", "zp_mget");

$times = 5;
while ($times--) {
  $ret = $zp_mget->Set("mget_key_".$times, "mget_value_".$times);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
}

$times = 5;
$keys = array();
while ($times--) {
  array_push($keys, "mget_key_".$times);
}

var_dump($keys);

$ret = $zp_mget->Mget($keys);
if ($ret == false) {
  echo "Mget Error". PHP_EOL;
}
var_dump($ret);
