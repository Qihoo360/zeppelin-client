<?php

$zp = new Zeppelin("127.0.0.1:9801", "example_test");
$times = 100;
while($times--) {
  $num = rand(10, 1000);
  $ret = $zp->Set("example_test_key", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
  $val = $zp->Get("example_test_key");
  if ($val == false || $val != $num) {
    echo "Error, num: ". $num. " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
  $ret = $zp->Delete("example_test_key");
  if ($ret == false) {
    echo "Delete Error". PHP_EOL;
    break;
  }

  $val = $zp->Get("example_test_key");
  if ($val != false) {
    echo "Error, expect false, but: ". " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
}
echo "done". PHP_EOL;
