import asyncio
import base64
import os

async def send_inline(reader, writer, cmd_str):
    """Send inline command and read response."""
    writer.write((cmd_str + "\r\n").encode())
    await writer.drain()
    resp = await reader.read(8192)
    return resp.decode(errors='replace').strip()

def parse_bulk(resp_str):
    """Parse RESP bulk string: $len\\r\\ndata"""
    if resp_str.startswith('$'):
        lines = resp_str.split('\r\n', 1)
        return lines[1] if len(lines) > 1 else resp_str
    return resp_str

async def run_test():
    print("=" * 60)
    print("  IgnisDB Compression & Encryption Test")
    print("=" * 60)
    
    reader, writer = await asyncio.open_connection('127.0.0.1', 6381)
    
    # Create test binary data (simulated 1KB image)
    image_data = b"\x89PNG\r\n\x1a\n" + os.urandom(100) + b"\x00" * 500 + os.urandom(100) + b"\xff" * 300
    raw_b64 = base64.b64encode(image_data).decode('ascii')
    
    print(f"\nOriginal Data: {len(image_data)} bytes")
    print(f"Raw Base64   : {len(raw_b64)} chars")
    
    # ─── Test 1: Compress Only (default) ───────────────────────
    print("\n── Test 1: SETBLOB with COMPRESS (default) ──")
    resp = await send_inline(reader, writer, f"SETBLOB img_compressed {raw_b64}")
    print(f"  SET Response: {resp}")
    
    # Check stored size
    stored = parse_bulk(await send_inline(reader, writer, "GET img_compressed"))
    print(f"  Stored size : {len(stored)} chars (vs {len(raw_b64)} raw b64)")
    savings = round((1 - len(stored) / len(raw_b64)) * 100, 1)
    print(f"  Savings     : {savings}%")
    
    # Retrieve via GETBLOB
    retrieved_b64 = parse_bulk(await send_inline(reader, writer, "GETBLOB img_compressed"))
    decoded = base64.b64decode(retrieved_b64)
    if decoded == image_data:
        print("  ✅ PASS: Compress → decompress → matches original!")
    else:
        print("  ❌ FAIL: Data mismatch!")
    
    # ─── Test 2: Compress + Encrypt ────────────────────────────
    print("\n── Test 2: SETBLOB with COMPRESS + ENCRYPT ──")
    resp = await send_inline(reader, writer, f"SETBLOB img_encrypted {raw_b64} COMPRESS ENCRYPT")
    print(f"  SET Response: {resp}")
    
    stored_enc = parse_bulk(await send_inline(reader, writer, "GET img_encrypted"))
    print(f"  Stored size : {len(stored_enc)} chars (encrypted)")
    
    # Verify raw GET is NOT the original (encrypted = different)
    if stored_enc != raw_b64:
        print("  ✅ Data is encrypted (differs from raw)")
    else:
        print("  ⚠  Data appears unencrypted")
    
    # Retrieve via GETBLOB (auto-decrypts)
    retrieved_b64 = parse_bulk(await send_inline(reader, writer, "GETBLOB img_encrypted"))
    decoded = base64.b64decode(retrieved_b64)
    if decoded == image_data:
        print("  ✅ PASS: Encrypt+Compress → decrypt+decompress → matches!")
    else:
        print(f"  ❌ FAIL: Data mismatch! Got {len(decoded)} bytes vs {len(image_data)}")
    
    # ─── Test 3: No compression ────────────────────────────────
    print("\n── Test 3: SETBLOB with NOCOMPRESS ──")
    resp = await send_inline(reader, writer, f"SETBLOB img_raw {raw_b64} NOCOMPRESS")
    print(f"  SET Response: {resp}")
    
    retrieved_b64 = parse_bulk(await send_inline(reader, writer, "GETBLOB img_raw"))
    decoded = base64.b64decode(retrieved_b64)
    if decoded == image_data:
        print("  ✅ PASS: Raw (no compress) roundtrip successful!")
    else:
        print("  ❌ FAIL: Data mismatch!")
    
    # ─── Test 4: Encrypt Only ──────────────────────────────────
    print("\n── Test 4: SETBLOB with ENCRYPT only (NOCOMPRESS) ──")
    resp = await send_inline(reader, writer, f"SETBLOB img_enc_only {raw_b64} NOCOMPRESS ENCRYPT")
    print(f"  SET Response: {resp}")
    
    retrieved_b64 = parse_bulk(await send_inline(reader, writer, "GETBLOB img_enc_only"))
    decoded = base64.b64decode(retrieved_b64)
    if decoded == image_data:
        print("  ✅ PASS: Encrypt-only roundtrip successful!")
    else:
        print(f"  ❌ FAIL: Data mismatch! Got {len(decoded)} bytes vs {len(image_data)}")
    
    # ─── Summary ───────────────────────────────────────────────
    print("\n── Storage Comparison ──")
    stored_raw = parse_bulk(await send_inline(reader, writer, "GET img_raw"))
    stored_comp = parse_bulk(await send_inline(reader, writer, "GET img_compressed"))
    stored_enc = parse_bulk(await send_inline(reader, writer, "GET img_encrypted"))
    print(f"  Raw Base64      : {len(raw_b64)} chars")
    print(f"  NOCOMPRESS      : {len(stored_raw)} chars")
    print(f"  COMPRESS        : {len(stored_comp)} chars  ({round(len(stored_comp)/len(raw_b64)*100,1)}%)")
    print(f"  COMPRESS+ENCRYPT: {len(stored_enc)} chars  ({round(len(stored_enc)/len(raw_b64)*100,1)}%)")
    
    # Cleanup
    for key in ['img_compressed', 'img_encrypted', 'img_raw', 'img_enc_only']:
        await send_inline(reader, writer, f"DELETE {key}")
    
    writer.close()
    await writer.wait_closed()
    
    print("\n" + "=" * 60)
    print("  All Tests Complete!")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_test())
