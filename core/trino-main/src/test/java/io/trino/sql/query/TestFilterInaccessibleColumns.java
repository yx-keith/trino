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
package io.trino.sql.query;

import com.google.common.collect.ImmutableMap;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.spi.security.Identity;
import io.trino.spi.security.ViewExpression;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestFilterInaccessibleColumns
{
    private static final String USER = "user";
    private static final String ADMIN = "admin";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(TEST_CATALOG_NAME)
            .setSchema(TINY_SCHEMA_NAME)
            .setSystemProperty("hide_inaccessible_columns", "true")
            .setIdentity(Identity.forUser(USER).build())
            .build();

    private final QueryAssertions assertions;
    private final TestingAccessControlManager accessControl;

    public TestFilterInaccessibleColumns()
    {
        LocalQueryRunner runner = LocalQueryRunner.builder(SESSION)
                .withFeaturesConfig(new FeaturesConfig().setHideInaccessibleColumns(true))
                .build();

        runner.createCatalog(TEST_CATALOG_NAME, new TpchConnectorFactory(1), ImmutableMap.of());
        assertions = new QueryAssertions(runner);
        accessControl = assertions.getQueryRunner().getAccessControl();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testSelectBaseline()
    {
        accessControl.reset();

        // No filtering baseline
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");
    }

    @Test
    public void testSimpleTableSchemaFilter()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");
    }

    @Test
    public void testDescribeBaseline()
    {
        accessControl.reset();

        assertThat(assertions.query("DESCRIBE nation"))
                .matches(materializedRows -> materializedRows
                        .getMaterializedRows().stream()
                        .anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testDescribe()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("DESCRIBE nation"))
                .matches(materializedRows -> materializedRows
                        .getMaterializedRows().stream()
                        .noneMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testShowColumnsBaseline()
    {
        accessControl.reset();

        assertThat(assertions.query("SHOW COLUMNS FROM nation"))
                .matches(materializedRows -> materializedRows
                        .getMaterializedRows().stream()
                        .anyMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    @Test
    public void testShowColumns()
    {
        accessControl.reset();

        accessControl.deny(privilege("nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SHOW COLUMNS FROM nation"))
                .matches(materializedRows -> materializedRows
                        .getMaterializedRows().stream()
                        .noneMatch(materializedRow -> materializedRow.getField(0).equals("comment")));
    }

    /**
     * Test filtering when columns are explicitly specified in SELECT
     */
    @Test
    public void testFilterExplicitSelect()
    {
        accessControl.reset();

        // Select the columns that are available to us explicitly
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SELECT nationkey, name, regionkey FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");

        // Select all columns explicitly
        assertThatThrownBy(() -> assertions.query("SELECT nationkey, name, regionkey, comment FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
    }

    @Test
    public void testRowFilterWithAccessToInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.rowFilter(new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation"),
                USER,
                ViewExpression.builder()
                        .identity(ADMIN)
                        .catalog(TEST_CATALOG_NAME)
                        .schema(TINY_SCHEMA_NAME)
                        .expression("comment IS NOT null")
                        .build());
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3')");
    }

    @Test
    public void testRowFilterWithoutAccessToInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.rowFilter(new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation"),
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(TEST_CATALOG_NAME)
                        .schema(TINY_SCHEMA_NAME)
                        .expression("comment IS NOT null")
                        .build());
        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
    }

    @Test
    public void testRowFilterAsSessionUserOnInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        QualifiedObjectName table = new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation");
        ViewExpression filter = ViewExpression.builder()
                .catalog(TEST_CATALOG_NAME)
                .schema(TINY_SCHEMA_NAME)
                .expression("comment IS NOT null")
                .build();
        accessControl.rowFilter(table, ADMIN, filter);
        accessControl.rowFilter(table, USER, filter);

        assertThatThrownBy(() -> assertions.query(user(USER), "SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
        assertThat(assertions.query(user(ADMIN), "SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '6', CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");
    }

    @Test
    public void testMaskingOnAccessibleColumn()
    {
        accessControl.reset();

        accessControl.columnMask(new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation"),
                "nationkey",
                USER,
                ViewExpression.builder()
                        .identity(ADMIN)
                        .catalog(TEST_CATALOG_NAME)
                        .schema(TINY_SCHEMA_NAME)
                        .expression("-nationkey")
                        .build());
        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (BIGINT '-6',CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('refully final requests. regular, ironi' AS VARCHAR(152)))");
    }

    @Test
    public void testMaskingWithoutAccessToInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.nationkey", SELECT_COLUMN));
        accessControl.columnMask(new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(USER)
                        .catalog(TEST_CATALOG_NAME)
                        .schema(TINY_SCHEMA_NAME)
                        .expression("CASE nationkey WHEN 6 THEN 'masked-comment' ELSE comment END")
                        .build());

        assertThatThrownBy(() -> assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
    }

    @Test
    public void testMaskingWithAccessToInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.nationkey", SELECT_COLUMN));
        accessControl.columnMask(new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation"),
                "comment",
                USER,
                ViewExpression.builder()
                        .identity(ADMIN)
                        .catalog(TEST_CATALOG_NAME)
                        .schema(TINY_SCHEMA_NAME)
                        .expression("CASE nationkey WHEN 6 THEN 'masked-comment' ELSE comment END")
                        .build());

        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .matches("VALUES (CAST('FRANCE' AS VARCHAR(25)), BIGINT '3', CAST('masked-comment' AS VARCHAR(152)))");

        assertThat(assertions.query("SELECT * FROM nation WHERE name = 'CANADA'"))
                .matches("VALUES (CAST('CANADA' AS VARCHAR(25)), BIGINT '1', CAST('eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold' AS VARCHAR(152)))");
    }

    @Test
    public void testMaskingAsSessionUserWithCaseOnInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.nationkey", SELECT_COLUMN));
        QualifiedObjectName table = new QualifiedObjectName(TEST_CATALOG_NAME, TINY_SCHEMA_NAME, "nation");
        ViewExpression mask = ViewExpression.builder()
                .catalog(TEST_CATALOG_NAME)
                .schema(TINY_SCHEMA_NAME)
                .expression("CASE nationkey WHEN 3 THEN 'masked-comment' ELSE comment END")
                .build();
        accessControl.columnMask(table, "comment", ADMIN, mask);
        accessControl.columnMask(table, "comment", USER, mask);

        assertThatThrownBy(() -> assertions.query(user(USER), "SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
        assertThat(assertions.query(user(ADMIN), "SELECT * FROM nation WHERE name = 'CANADA'"))
                .matches("VALUES (BIGINT '3', CAST('CANADA' AS VARCHAR(25)), BIGINT '1', CAST('masked-comment' AS VARCHAR(152)))");
    }

    @Test
    public void testPredicateOnInaccessibleColumn()
    {
        accessControl.reset();

        // Hide name but use it in the query predicate
        accessControl.deny(privilege(USER, "nation.name", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.query("SELECT * FROM nation WHERE name = 'FRANCE'"))
                .hasMessage("Access Denied: Cannot select from columns [nationkey, regionkey, name, comment] in table or view test-catalog.tiny.nation");
    }

    @Test
    public void testJoinBaseline()
    {
        accessControl.reset();

        assertThat(assertions.query("SELECT * FROM nation,customer WHERE customer.nationkey = nation.nationkey AND nation.name = 'FRANCE' AND customer.name='Customer#000001477'"))
                .matches(materializedRows ->
                    materializedRows.getMaterializedRows().get(0).getField(11).equals("ites nag blithely alongside of the ironic accounts. accounts use. carefully silent deposits"));
    }

    @Test
    public void testJoin()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.comment", SELECT_COLUMN));
        assertThat(assertions.query("SELECT * FROM nation,customer WHERE customer.nationkey = nation.nationkey AND nation.name = 'FRANCE' AND customer.name='Customer#000001477'"))
                .matches(materializedRows ->
                        materializedRows.getMaterializedRows().get(0).getFields().size() == 11);
    }

    @Test
    public void testConstantFields()
    {
        accessControl.reset();

        assertThat(assertions.query("SELECT * FROM (SELECT 'test')"))
                .matches("VALUES ('test')");
    }

    @Test
    public void testFunctionFields()
    {
        accessControl.reset();

        assertThat(assertions.query("SELECT * FROM (SELECT concat(name,'-test') FROM nation WHERE name = 'FRANCE')"))
                .matches("VALUES (CAST('FRANCE-test' AS VARCHAR))");
    }

    @Test
    public void testFunctionOnInaccessibleColumn()
    {
        accessControl.reset();

        accessControl.deny(privilege(USER, "nation.name", SELECT_COLUMN));
        assertThatThrownBy(() -> assertions.query("SELECT * FROM (SELECT concat(name,'-test') FROM nation WHERE name = 'FRANCE')"))
                .hasMessage("Access Denied: Cannot select from columns [name] in table or view test-catalog.tiny.nation");
    }

    private Session user(String user)
    {
        return Session.builder(assertions.getDefaultSession())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
