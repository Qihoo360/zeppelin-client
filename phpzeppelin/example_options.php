<?php

$timeout = 100;
$zp_options = new Zeppelin("127.0.0.1:9801;127.0.0.1:9802;127.0.0.1:9803", "example_options_test", $timeout);

$times = 200;
while($times--) {
  $num = rand(10, 1000);
  $ret = $zp_options->Set("example_options_test_key", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
  $val = $zp_options->Get("example_options_test_key");
  if ($val == false || $val != $num) {
    echo "Error, num: ". $num. " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }

  $ret = $zp_options->Delete("example_options_test_key");
  if ($ret == false) {
    echo "Delete Error". PHP_EOL;
    break;
  }

  $val = $zp_options->Get("example_options_test_key");
  if ($val != false) {
    echo "Error, expect false, but: ". " val: ". $val. PHP_EOL;
    var_dump($val);
    break;
  }
  $ret = $zp_options->Set("example_options_test_key", $num);
  if ($ret == false) {
    echo "Set Error". PHP_EOL;
    break;
  }
}

echo "done". PHP_EOL;
