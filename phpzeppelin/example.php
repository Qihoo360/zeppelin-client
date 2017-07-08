<?php

$metas["127.0.0.1"] ="9801";
$metas["127.0.0.1"] ="9802";
$timeout = 100

$zp_options = new Zeppelin($metas, "options_test1", $timeout);

$times = 200;
while($times--) {
  $num = rand(10, 1000);
  $ret = $zp_options->Set("options_key1", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
  $val = $zp_options->Get("options_key1");
  if ($val == false || $val != $num) {
    echo "Error, num: ". $num. " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }

  $ret = $zp_options->Delete("options_key1");
  if ($ret == false) {
    echo "Delete Error". PHP_EOL;
    break;
  }

  $val = $zp_options->Get("options_key1");
  if ($val != false) {
    echo "Error, expect false, but: ". " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
}

$zp = new Zeppelin("127.0.0.1", 9801, "test1");
$times = 100;
while($times--) {
  $num = rand(10, 1000);
  $ret = $zp->Set("key1", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
  $val = $zp->Get("key1");
  if ($val == false || $val != $num) {
    echo "Error, num: ". $num. " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
  $ret = $zp->Delete("key1");
  if ($ret == false) {
    echo "Delete Error". PHP_EOL;
    break;
  }

  $val = $zp->Get("key1");
  if ($val != false) {
    echo "Error, expect false, but: ". " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
}

$zp_mget = new Zeppelin("127.0.0.1", 9801, "mget_test");
// Mget
$times = 5;
while ($times--) {
  $ret = $zp_mget->Set("key".$times, $times);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
}

$times = 5;
$keys = array();
while ($times--) {
  array_push($keys, "key".$times);
}

var_dump($keys);

$ret = $zp_mget->Mget($keys);
if ($ret == false) {
  echo "Mget Error". PHP_EOL;
}
var_dump($ret);

echo "done". PHP_EOL;
