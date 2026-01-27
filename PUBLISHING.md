# Publishing to Maven Central

This document describes how to publish ParquetKT to Maven Central using GitLab CI.

## Prerequisites

### 1. Sonatype OSSRH Account

1. Create an account at [Sonatype JIRA](https://issues.sonatype.org)
2. Create a ticket to claim the `com.molo17` namespace
3. Wait for approval (usually 1-2 business days)
4. Save your username and password

### 2. GPG Key for Signing

Generate a GPG key pair for signing artifacts:

```bash
# Generate a new GPG key
gpg --gen-key

# List your keys to get the key ID
gpg --list-secret-keys --keyid-format=long

# Export your private key (base64 encoded for GitLab CI)
gpg --export-secret-keys YOUR_KEY_ID | base64 > gpg-private-key.txt

# Publish your public key to a key server
gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
```

### 3. GitLab CI/CD Variables

Configure the following variables in GitLab (Settings → CI/CD → Variables):

| Variable Name | Description | Protected | Masked |
|--------------|-------------|-----------|---------|
| `SONATYPE_USERNAME` | Your Sonatype JIRA username | ✓ | ✓ |
| `SONATYPE_PASSWORD` | Your Sonatype JIRA password | ✓ | ✓ |
| `GPG_PRIVATE_KEY` | Base64-encoded GPG private key | ✓ | ✓ |
| `GPG_KEY_ID` | Your GPG key ID (last 8 chars) | ✓ | - |
| `GPG_PASSPHRASE` | Your GPG key passphrase | ✓ | ✓ |

**Important:** Mark all variables as "Protected" so they're only available on protected branches/tags.

## Publishing Process

### Release Version (to Maven Central)

1. **Update version** in `build.gradle.kts`:
   ```kotlin
   version = "1.0.0"  // Remove -SNAPSHOT
   ```

2. **Commit and push** the version change:
   ```bash
   git add build.gradle.kts
   git commit -m "Release version 1.0.0"
   git push origin main
   ```

3. **Create and push a tag**:
   ```bash
   git tag -a v1.0.0 -m "Release version 1.0.0"
   git push origin v1.0.0
   ```

4. **GitLab CI will automatically**:
   - Build the project
   - Run all tests
   - Sign the artifacts with GPG
   - Publish to Sonatype staging repository
   - Close and release the staging repository
   - Sync to Maven Central (takes ~10-30 minutes)

5. **Verify publication**:
   - Check [Maven Central](https://central.sonatype.com/artifact/com.molo17/parquetkt)
   - It may take up to 2 hours for the artifact to appear in search

### Snapshot Version (for development)

Snapshot versions are automatically published on every push to `main` branch:

1. Ensure version ends with `-SNAPSHOT` in `build.gradle.kts`:
   ```kotlin
   version = "1.0.0-SNAPSHOT"
   ```

2. Push to main branch:
   ```bash
   git push origin main
   ```

3. Snapshot will be available at:
   ```
   https://s01.oss.sonatype.org/content/repositories/snapshots/com/molo17/parquetkt/
   ```

## Using the Published Library

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("com.molo17:parquetkt:1.0.0")
}
```

### Gradle (Groovy DSL)

```groovy
dependencies {
    implementation 'com.molo17:parquetkt:1.0.0'
}
```

### Maven

```xml
<dependency>
    <groupId>com.molo17</groupId>
    <artifactId>parquetkt</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Troubleshooting

### GPG Signing Issues

If you encounter GPG signing errors:

1. Verify your GPG key is properly base64 encoded:
   ```bash
   gpg --export-secret-keys YOUR_KEY_ID | base64 | tr -d '\n' > gpg-key.txt
   ```

2. Ensure the key ID matches the one in your GPG keyring:
   ```bash
   gpg --list-secret-keys --keyid-format=long
   ```

3. Test GPG import locally:
   ```bash
   echo "$GPG_PRIVATE_KEY" | base64 -d | gpg --batch --import
   ```

### Sonatype Authentication Issues

- Verify your credentials at [Sonatype OSSRH](https://s01.oss.sonatype.org/)
- Ensure your account has permission for the `com.molo17` namespace
- Check that variables are marked as "Protected" in GitLab

### Publication Not Appearing

- Check the GitLab CI job logs for errors
- Verify the staging repository was closed and released
- Wait up to 2 hours for Maven Central sync
- Check [Sonatype Repository Manager](https://s01.oss.sonatype.org/) for staging repositories

## Manual Publishing (Local)

For testing or manual releases:

```bash
# Set environment variables
export SONATYPE_USERNAME="your-username"
export SONATYPE_PASSWORD="your-password"
export GPG_KEY_ID="your-key-id"

# Publish to staging
./gradlew publishToSonatype

# Close and release staging repository
./gradlew closeAndReleaseSonatypeStagingRepository
```

## References

- [Sonatype OSSRH Guide](https://central.sonatype.org/publish/publish-guide/)
- [Gradle Nexus Publish Plugin](https://github.com/gradle-nexus/publish-plugin)
- [GPG Signing Guide](https://central.sonatype.org/publish/requirements/gpg/)
