# GPG Setup Guide for Maven Central Publishing

This guide walks you through generating GPG keys and extracting the values needed for GitLab CI/CD.

## Step 1: Generate a GPG Key

```bash
# Generate a new GPG key (if you don't have one)
gpg --full-generate-key
```

When prompted:
- **Key type**: Choose `(1) RSA and RSA` (default)
- **Key size**: Enter `4096` for maximum security
- **Expiration**: Choose `0` (key does not expire) or set an expiration date
- **Real name**: Your full name (e.g., "Daniele Angeli")
- **Email**: Your email address (e.g., "daniele@molo17.com")
- **Comment**: Optional (e.g., "MOLO17 Code Signing")
- **Passphrase**: Choose a strong passphrase and remember it!

## Step 2: List Your GPG Keys

```bash
# List all secret keys with long format
gpg --list-secret-keys --keyid-format=long
```

Output will look like:
```
sec   rsa4096/8A1B2C3D4E5F6789 2026-01-27 [SC]
      ABCDEF1234567890ABCDEF1234567890ABCDEF12
uid                 [ultimate] Daniele Angeli (MOLO17 Code Signing) <daniele@molo17.com>
ssb   rsa4096/9876543210FEDCBA 2026-01-27 [E]
```

**Important**: The key ID is the part after `rsa4096/` on the `sec` line.
In this example: `8A1B2C3D4E5F6789`

## Step 3: Extract GPG_KEY_ID

From the output above, take the **last 8 characters** of the key ID:

```bash
# Your GPG_KEY_ID is the last 8 characters
# Example: 4E5F6789
```

**For GitLab CI/CD Variable:**
- **Name**: `GPG_KEY_ID`
- **Value**: `4E5F6789` (your actual last 8 characters)
- **Protected**: ✓ Yes
- **Masked**: No (it's not sensitive)

## Step 4: Extract GPG_PRIVATE_KEY (Base64 Encoded)

```bash
# Export your private key and encode it as base64
# Replace YOUR_KEY_ID with your actual key ID (e.g., 8A1B2C3D4E5F6789)
gpg --export-secret-keys YOUR_KEY_ID | base64 | tr -d '\n' > gpg-private-key.txt

# View the encoded key
cat gpg-private-key.txt
```

This will create a file `gpg-private-key.txt` with a long base64 string like:
```
lQdGBGXuABkBEAC8xK9vN2... (very long string) ...==
```

**For GitLab CI/CD Variable:**
- **Name**: `GPG_PRIVATE_KEY`
- **Value**: Copy the entire contents of `gpg-private-key.txt`
- **Protected**: ✓ Yes
- **Masked**: ✓ Yes

**Security Note**: Delete `gpg-private-key.txt` after copying to GitLab:
```bash
rm gpg-private-key.txt
```

## Step 5: Extract GPG_PASSPHRASE

This is the passphrase you entered when creating the GPG key in Step 1.

**For GitLab CI/CD Variable:**
- **Name**: `GPG_PASSPHRASE`
- **Value**: Your GPG key passphrase
- **Protected**: ✓ Yes
- **Masked**: ✓ Yes

## Step 6: Publish Your Public Key

Your public key needs to be available on key servers so Maven Central can verify signatures:

```bash
# Replace YOUR_KEY_ID with your actual key ID
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID

# Also send to other popular keyservers for redundancy
gpg --keyserver keys.openpgp.org --send-keys YOUR_KEY_ID
gpg --keyserver pgp.mit.edu --send-keys YOUR_KEY_ID
```

## Step 7: Verify Your Setup

Test that you can import your key from the base64 string:

```bash
# Test decoding and importing (this won't affect your keyring)
cat gpg-private-key.txt | base64 -d | gpg --list-packets
```

If this shows key information, your base64 encoding is correct.

## Complete Example

Here's a complete example of the process:

```bash
# 1. Generate key (if needed)
gpg --full-generate-key

# 2. List keys and find your key ID
gpg --list-secret-keys --keyid-format=long
# Output: sec   rsa4096/8A1B2C3D4E5F6789 ...
# Your full key ID: 8A1B2C3D4E5F6789
# Your GPG_KEY_ID (last 8 chars): 4E5F6789

# 3. Export private key as base64
gpg --export-secret-keys 8A1B2C3D4E5F6789 | base64 | tr -d '\n' > gpg-private-key.txt

# 4. Publish public key
gpg --keyserver keyserver.ubuntu.com --send-keys 8A1B2C3D4E5F6789

# 5. Copy to GitLab CI/CD variables:
#    - GPG_KEY_ID: 4E5F6789
#    - GPG_PRIVATE_KEY: (contents of gpg-private-key.txt)
#    - GPG_PASSPHRASE: (your passphrase)

# 6. Clean up
rm gpg-private-key.txt
```

## Adding Variables to GitLab

1. Go to your GitLab project
2. Navigate to **Settings → CI/CD → Variables**
3. Click **Add variable** for each of the three variables:

### GPG_KEY_ID
- **Key**: `GPG_KEY_ID`
- **Value**: `4E5F6789` (your last 8 characters)
- **Type**: Variable
- **Environment scope**: All
- **Protect variable**: ✓ (checked)
- **Mask variable**: ☐ (unchecked)
- **Expand variable reference**: ☐ (unchecked)

### GPG_PRIVATE_KEY
- **Key**: `GPG_PRIVATE_KEY`
- **Value**: (paste entire contents of gpg-private-key.txt)
- **Type**: Variable
- **Environment scope**: All
- **Protect variable**: ✓ (checked)
- **Mask variable**: ✓ (checked)
- **Expand variable reference**: ☐ (unchecked)

### GPG_PASSPHRASE
- **Key**: `GPG_PASSPHRASE`
- **Value**: (your GPG passphrase)
- **Type**: Variable
- **Environment scope**: All
- **Protect variable**: ✓ (checked)
- **Mask variable**: ✓ (checked)
- **Expand variable reference**: ☐ (unchecked)

## Troubleshooting

### "gpg: no valid OpenPGP data found"
- Make sure you're using the correct key ID
- Verify the base64 encoding with `base64 -d` test

### "gpg: decryption failed: No secret key"
- The key ID doesn't match the exported key
- Re-export with the correct key ID

### "gpg: signing failed: Inappropriate ioctl for device"
- Add `export GPG_TTY=$(tty)` to your shell profile
- Or use `echo "test" | gpg --clearsign` to test

### Key not found on keyserver
- Wait a few minutes after uploading
- Try multiple keyservers
- Verify with: `gpg --keyserver keyserver.ubuntu.com --recv-keys YOUR_KEY_ID`

## Security Best Practices

1. **Never commit** GPG keys to version control
2. **Use strong passphrases** (at least 20 characters)
3. **Backup your keys** securely (encrypted external drive)
4. **Set expiration dates** and renew keys periodically
5. **Revoke old keys** when no longer needed
6. **Use protected variables** in GitLab CI/CD
7. **Limit access** to GitLab project settings

## Backup Your Keys

```bash
# Backup private key (encrypted)
gpg --export-secret-keys --armor YOUR_KEY_ID > gpg-private-backup.asc

# Backup public key
gpg --export --armor YOUR_KEY_ID > gpg-public-backup.asc

# Store these files securely (encrypted USB drive, password manager, etc.)
```

## Restore Keys on Another Machine

```bash
# Import private key
gpg --import gpg-private-backup.asc

# Import public key
gpg --import gpg-public-backup.asc

# Trust the key
gpg --edit-key YOUR_KEY_ID
# In GPG prompt: trust → 5 (ultimate) → quit
```
