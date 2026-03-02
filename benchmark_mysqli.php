<?php
// benchmark_mysqli.php

$host = '127.0.0.1';
$port = 3307; // IgnisDB MySQL Layer
$user = 'ignis_user';
$pass = 'ignis_pass';
$db = 'ignis_db';
$iterations = 10000;

echo "Connecting to IgnisDB (MySQL Protocol) at $host:$port...\n";

$start_time = microtime(true);

$mysqli = new mysqli($host, $user, $pass, $db, $port);

if ($mysqli->connect_error) {
    die("Connection failed: " . $mysqli->connect_error . "\n");
}

echo "Connected! Benchmarking SELECT (GET) ...\n";

// Warmup
for ($i = 0; $i < 100; $i++) {
    $mysqli->query("SELECT * FROM data_users WHERE id = '1'");
}

$start_benchmark = microtime(true);
$found_count = 0;
$first_result_printed = false;

for ($i = 0; $i < $iterations; $i++) {
    $result = $mysqli->query("SELECT * FROM data_users WHERE id = '1'");
    if ($result) {
        $row = $result->fetch_assoc();
        if ($row) {
            $found_count++;
            if (!$first_result_printed) {
                echo "First Result Sample: " . json_encode($row) . "\n\n";
                $first_result_printed = true;
            }
        }
        $result->free();
    }
}

$end_benchmark = microtime(true);
$total_time = $end_benchmark - $start_benchmark;

echo "\nResults:\n";
echo "  Iterations: $iterations\n";
echo "  Total Time: " . number_format($total_time, 4) . " s\n";
echo "  Throughput: " . number_format($iterations / $total_time, 0) . " req/s\n";
echo "  Avg Latency: " . number_format(($total_time / $iterations) * 1000, 4) . " ms\n";

$mysqli->close();
?>