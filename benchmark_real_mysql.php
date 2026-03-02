<?php
// benchmark_real_mysql.php

// Update these with your LOCAL/REAL MySQL credentials
$host = '127.0.0.1';
$port = 3306; // Default MySQL Port
$user = 'lirakazan_admin';
$pass = 'EAeDbotyyg2m54_^';
$db = 'lirakazan_veritabani'; // Change to your real DB name

$iterations = 10000;

echo "Connecting to REAL MySQL at $host:$port...\n";

$mysqli = new mysqli($host, $user, $pass, $db, $port);

if ($mysqli->connect_error) {
    die("Connection failed: " . $mysqli->connect_error . "\n");
}

echo "Connected! Benchmarking SELECT (Real MySQL) ...\n";

// Warmup
for ($i = 0; $i < 100; $i++) {
    $mysqli->query("SELECT * FROM data_users WHERE id = '1'");
}

$start_benchmark = microtime(true);
$found_count = 0;

for ($i = 0; $i < $iterations; $i++) {
    // Assuming table 'data_users' exists in your real MySQL with a row id=1
    $result = $mysqli->query("SELECT * FROM data_users WHERE id = '1'");
    if ($result) {
        $row = $result->fetch_assoc();
        if ($row)
            $found_count++;
        $result->free();
    }
}

$end_benchmark = microtime(true);
$total_time = $end_benchmark - $start_benchmark;

echo "\nResults (Real MySQL):\n";
echo "  Iterations: $iterations\n";
echo "  Total Time: " . number_format($total_time, 4) . " s\n";
echo "  Throughput: " . number_format($iterations / $total_time, 0) . " req/s\n";
echo "  Avg Latency: " . number_format(($total_time / $iterations) * 1000, 4) . " ms\n";

$mysqli->close();
?>