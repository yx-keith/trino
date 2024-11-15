/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.security;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.server.InternalAuthenticationManager;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import jakarta.annotation.Priority;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.password.file.EncryptionUtil.doesBCryptPasswordMatch;
import static io.trino.server.ServletSecurityUtils.sendWwwAuthenticate;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static jakarta.ws.rs.Priorities.AUTHENTICATION;
import static java.util.Objects.requireNonNull;

@Priority(AUTHENTICATION)
public class AuthenticationFilter
        implements ContainerRequestFilter
{
    private final List<Authenticator> authenticators;
    private final InternalAuthenticationManager internalAuthenticationManager;
    private final boolean insecureAuthenticationOverHttpAllowed;
    private final InsecureAuthenticator insecureAuthenticator;
    private String hashedPassword;
    private final boolean httpserverPasswdEnabled;

    @Inject
    public AuthenticationFilter(
            List<Authenticator> authenticators,
            InternalAuthenticationManager internalAuthenticationManager,
            SecurityConfig securityConfig,
            InsecureAuthenticator insecureAuthenticator)
    {
        this.authenticators = ImmutableList.copyOf(requireNonNull(authenticators, "authenticators is null"));
        checkArgument(!authenticators.isEmpty(), "authenticators is empty");
        this.internalAuthenticationManager = requireNonNull(internalAuthenticationManager, "internalAuthenticationManager is null");
        insecureAuthenticationOverHttpAllowed = securityConfig.isInsecureAuthenticationOverHttpAllowed();
        this.insecureAuthenticator = requireNonNull(insecureAuthenticator, "insecureAuthenticator is null");
        this.httpserverPasswdEnabled = securityConfig.isHttpserverPasswdEnabled();
        if (httpserverPasswdEnabled) {
            List<String> lines = readPasswordFile(securityConfig.getPasswordFile());
            this.hashedPassword = loadPasswordFile(lines);
        }
    }

    @Override
    public void filter(ContainerRequestContext request)
    {
        if (InternalAuthenticationManager.isInternalRequest(request)) {
            internalAuthenticationManager.handleInternalRequest(request);
            return;
        }

        List<Authenticator> authenticators;
        if (request.getSecurityContext().isSecure()) {
            authenticators = this.authenticators;
        }
        else if (insecureAuthenticationOverHttpAllowed) {
            authenticators = ImmutableList.of(insecureAuthenticator);
        }
        else {
            throw new ForbiddenException("Authentication over HTTP is not enabled");
        }

        // try to authenticate, collecting errors and authentication headers
        Set<String> messages = new LinkedHashSet<>();
        Set<String> authenticateHeaders = new LinkedHashSet<>();

        for (Authenticator authenticator : authenticators) {
            Identity authenticatedIdentity;
            try {
                if (authenticator instanceof InsecureAuthenticator) {
                    if (httpserverPasswdEnabled){
                        Optional<BasicAuthCredentials> basicAuthCredentials = BasicAuthCredentials.extractBasicAuthCredentials(request);
                        if (!basicAuthCredentials.isPresent()) {
                            throw new AuthenticationException("Password is null. Please enter your password when starting");
                        }
                        String inputPassword = basicAuthCredentials.get().getPassword().get();

                        if(!doesBCryptPasswordMatch(inputPassword, hashedPassword)) {
                            throw new AuthenticationException("password is not correct");
                        }
                    }
                }
                authenticatedIdentity = authenticator.authenticate(request);
            }
            catch (AuthenticationException e) {
                // Some authenticators (e.g. password) nest multiple internal authenticators.
                // Exceptions from additional failed login attempts are suppressed in the first exception
                Stream.concat(Stream.of(e), Arrays.stream(e.getSuppressed()))
                        .filter(ex -> ex instanceof AuthenticationException)
                        .map(AuthenticationException.class::cast)
                        .forEach(ex -> {
                            if (ex.getMessage() != null) {
                                messages.add(ex.getMessage());
                            }
                            ex.getAuthenticateHeader().ifPresent(authenticateHeaders::add);
                        });
                continue;
            }

            // authentication succeeded
            setAuthenticatedIdentity(request, authenticatedIdentity);
            return;
        }

        // authentication failed
        if (messages.isEmpty()) {
            messages.add("Unauthorized");
        }
        // The error string is used by clients for exception messages and
        // is presented to the end user, thus it should be a single line.
        String error = Joiner.on(" | ").join(messages);

        sendWwwAuthenticate(request, error, authenticateHeaders);
    }

    public static String loadPasswordFile(List<String> lines)
    {

        String line = lines.get(0).trim();

        List<String> parts = Splitter.on(":").limit(2).splitToList(line);
        if (parts.size() != 2) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Expected two parts for user and password");
        }
        String user = parts.get(0);
        String hashedPassword = parts.get(1);

        return hashedPassword;
    }

    public static List<String> readPasswordFile(File file)
    {
        try {
            return Files.readAllLines(file.toPath());
        }
        catch (IOException e) {
            throw new TrinoException(CONFIGURATION_UNAVAILABLE, "Failed to read password file: " + file, e);
        }
    }
}
