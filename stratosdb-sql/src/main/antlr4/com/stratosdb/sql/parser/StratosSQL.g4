grammar StratosSQL;

// Parser rules
parse: sqlStatement EOF;

sqlStatement: createTable | createIndex | dropIndex | insert | selectWithCte | select | update | delete | dropTable | showTables | showStats | showTableStats | showStatements | showActivity | showTransactionIsolationLevel | showParameter | setParameter | setTransaction | setSessionCharacteristics | showCatalog | explain | analyze | vacuum | reindexStatement | beginTxn | commitTxn | rollbackTxn | createView | dropView | savepoint | releaseSavepoint | rollbackToSavepoint | createSequence | alterSequence | dropSequence | createType | dropType | createFunction | dropFunction | createProcedure | dropProcedure | callStatement | createTrigger | dropTrigger | createExtension | dropExtension | createNativeFunction | alterTableAddColumn | alterTableDropColumn | alterTableRenameColumn | alterTableRenameTable | alterTableAlterColumnType | alterTableSetDefault | alterTableDropDefault | alterTableAddConstraint | alterTableDropConstraint | alterTableEnableRls | alterTableDisableRls | alterTableForceRls | createPolicy | dropPolicy | createRole | dropRole | grantStatement | revokeStatement | copyStatement | checkpointStatement | promoteStatement | createDatabase | dropDatabase | showDatabases;

// --- Real procedural language ("LANGUAGE plpgsql") - a real, second, wholly
// independent parse entry point using this SAME lexer (see PlpgsqlParser),
// invoked ONLY when interpreting a function/procedure body declared with
// this language, never as part of ordinary top-level SQL statement parsing
// (sqlStatement above never references plpgsqlBlock at all). Real control
// flow this engine previously had none of at all: IF/ELSIF/ELSE, WHILE and
// FOR-range loops, a plain LOOP with EXIT/CONTINUE, local variable
// DECLARE/assignment, and RETURN - closing a real, named gap from real
// PL/pgSQL, not just a naming difference (see PlpgsqlInterpreter's own
// javadoc for the full, honest scope and limitations).
plpgsqlBlock: plpgsqlDeclareSection? BEGIN plpgsqlStatement* PLSQL_END SEMICOLON? EOF;

plpgsqlDeclareSection: PLSQL_DECLARE plpgsqlVarDecl+;
plpgsqlVarDecl: IDENTIFIER dataType (ASSIGN_PLSQL plpgsqlExpr | DEFAULT plpgsqlExpr)? SEMICOLON;

plpgsqlStatement: plpgsqlIfStatement
                | plpgsqlWhileStatement
                | plpgsqlForRangeStatement
                | plpgsqlLoopStatement
                | plpgsqlExitStatement
                | plpgsqlContinueStatement
                | plpgsqlReturnStatement
                | plpgsqlRaiseStatement
                | plpgsqlAssignment
                | plpgsqlEmbeddedSql
                ;

plpgsqlAssignment: IDENTIFIER ASSIGN_PLSQL plpgsqlExpr SEMICOLON;

plpgsqlIfStatement: PLSQL_IF plpgsqlExpr PLSQL_THEN plpgsqlStatement*
                    (PLSQL_ELSIF plpgsqlExpr PLSQL_THEN plpgsqlStatement*)*
                    (PLSQL_ELSE plpgsqlStatement*)?
                    PLSQL_END PLSQL_IF SEMICOLON;

plpgsqlWhileStatement: PLSQL_WHILE plpgsqlExpr PLSQL_LOOP plpgsqlStatement* PLSQL_END PLSQL_LOOP SEMICOLON;

plpgsqlForRangeStatement: FOR IDENTIFIER IN plpgsqlExpr DOTDOT plpgsqlExpr PLSQL_LOOP plpgsqlStatement* PLSQL_END PLSQL_LOOP SEMICOLON;

plpgsqlLoopStatement: PLSQL_LOOP plpgsqlStatement* PLSQL_END PLSQL_LOOP SEMICOLON;

plpgsqlExitStatement: PLSQL_EXIT (PLSQL_WHEN plpgsqlExpr)? SEMICOLON;
plpgsqlContinueStatement: PLSQL_CONTINUE (PLSQL_WHEN plpgsqlExpr)? SEMICOLON;

plpgsqlReturnStatement: PLSQL_RETURN plpgsqlExpr? SEMICOLON;

plpgsqlRaiseStatement: PLSQL_RAISE (PLSQL_NOTICE | PLSQL_EXCEPTION | PLSQL_WARNING)? STRING_LITERAL SEMICOLON;

// A real, embedded SQL statement (INSERT/UPDATE/DELETE/a real SELECT ... INTO
// a local variable) - captured here as a raw run of tokens up to its own
// terminating semicolon, THEN handed to the real, existing SqlParser for
// real, actual parsing (after real variable substitution - see
// PlpgsqlInterpreter's own javadoc) rather than re-implementing SQL parsing
// a second time inside this grammar. plpgsqlToken matches any single token
// that isn't itself a semicolon, so this rule greedily consumes everything
// up to (but not including) the terminator, regardless of what real SQL
// statement type it turns out to be.
plpgsqlEmbeddedSql: plpgsqlToken+ SEMICOLON;
// Excludes EVERY real plpgsql structural keyword, not just ELSIF/ELSE/END -
// a real, second bug found by testing, not by inspection, right after fixing
// the first one: a genuinely malformed IF (e.g. missing its own END IF) was
// being silently swallowed whole as one opaque, generic plpgsqlEmbeddedSql
// statement instead of failing with a real, clear syntax error - since IF
// itself wasn't excluded, the wildcard happily matched right through it.
// This completely defeated real, upfront CREATE-time body validation (see
// ExecutorEngine.validateFunctionOrProcedureLanguage): the malformed body
// was silently ACCEPTED at CREATE time, then failed with a confusing,
// unrelated "mismatched input 'IF'" SQL syntax error only at CALL time
// instead. Excluding every real structural keyword here (not just the three
// that happened to trigger the first, narrower bug) closes this properly.
plpgsqlToken: ~(SEMICOLON | PLSQL_IF | PLSQL_THEN | PLSQL_ELSIF | PLSQL_ELSE | PLSQL_END
              | PLSQL_WHILE | PLSQL_LOOP | PLSQL_EXIT | PLSQL_CONTINUE | PLSQL_RETURN
              | PLSQL_RAISE | PLSQL_WHEN | FOR);

// A minimal, real, deliberately non-exhaustive expression language for the
// procedural block itself - literals, variables, arithmetic, comparison,
// boolean logic, parentheses, and a function call. Alternative order here
// IS this rule's own real operator precedence (ANTLR4's own direct-left-
// recursion precedence climbing): multiplication/division bind tightest,
// then addition/subtraction, then comparison, then NOT, then AND, then OR
// loosest - the same real, standard precedence order every mainstream
// procedural/SQL language uses.
plpgsqlExpr: plpgsqlExpr op=(STAR | DIVIDE) plpgsqlExpr                    #PlpgsqlMulDiv
           | plpgsqlExpr op=(PLUS | MINUS) plpgsqlExpr                     #PlpgsqlAddSub
           | plpgsqlExpr op=(GT | LT | GE | LE | ASSIGN | NE) plpgsqlExpr  #PlpgsqlCompare
           | NOT plpgsqlExpr                                                #PlpgsqlNot
           | plpgsqlExpr AND plpgsqlExpr                                    #PlpgsqlAnd
           | plpgsqlExpr OR plpgsqlExpr                                     #PlpgsqlOr
           | MINUS plpgsqlExpr                                              #PlpgsqlNegate
           | LPAREN plpgsqlExpr RPAREN                                      #PlpgsqlParen
           | IDENTIFIER LPAREN (plpgsqlExpr (COMMA plpgsqlExpr)*)? RPAREN   #PlpgsqlFunctionCall
           | IDENTIFIER                                                     #PlpgsqlVariable
           | literal                                                        #PlpgsqlLiteralExpr
           ;

// DDL
createTable: CREATE TABLE (PLSQL_IF NOT EXISTS)? tableName LPAREN (columnDef | tableConstraint) (COMMA (columnDef | tableConstraint))* RPAREN SEMICOLON?;
tableConstraint: (CONSTRAINT constraintName)? PRIMARY KEY LPAREN columnNameList RPAREN
               | (CONSTRAINT constraintName)? UNIQUE LPAREN columnNameList RPAREN
               | (CONSTRAINT constraintName)? FOREIGN KEY LPAREN childCols=columnNameList RPAREN REFERENCES tableName (LPAREN parentCols=columnNameList RPAREN)? fkAction*
               | (CONSTRAINT constraintName)? CHECK LPAREN expression RPAREN
               ;
columnNameList: columnName (COMMA columnName)*;
fkAction: ON DELETE fkActionType
        | ON UPDATE fkActionType
        ;
fkActionType: CASCADE
            | RESTRICT
            | SET NULL
            | NO ACTION
            ;
constraintName: anyIdentifier;
createIndex: CREATE (UNIQUE)? INDEX (PLSQL_IF NOT EXISTS)? indexName ON tableName LPAREN columnName (COMMA columnName)? RPAREN (USING (HASH | BTREE | BRIN | GIN | BITMAP | GIST))? SEMICOLON?;
dropIndex: DROP INDEX (PLSQL_IF EXISTS)? indexName SEMICOLON?;
dropTable: DROP TABLE (PLSQL_IF EXISTS)? tableName SEMICOLON?;
createDatabase: CREATE DATABASE databaseName SEMICOLON?;
dropDatabase: DROP DATABASE databaseName SEMICOLON?;
showDatabases: SHOW DATABASES SEMICOLON?;
databaseName: anyIdentifier;
copyStatement: COPY tableName (LPAREN columnName (COMMA columnName)* RPAREN)? (FROM | TO) copyTarget (WITH? LPAREN copyOption (COMMA copyOption)* RPAREN)? SEMICOLON?;
copyTarget: STRING_LITERAL | STDIN | STDOUT;
copyOption: FORMAT (TEXT | CSV) | DELIMITER STRING_LITERAL | HEADER (TRUE | FALSE)? | NULL STRING_LITERAL;
createRole: CREATE ROLE roleName (WITH)? roleOption* SEMICOLON?;
roleOption: LOGIN | NOLOGIN | SUPERUSER | NOSUPERUSER | PASSWORD STRING_LITERAL;
dropRole: DROP ROLE roleName SEMICOLON?;
grantStatement: grantTable | grantDatabase | grantRole;
revokeStatement: revokeTable | revokeDatabase | revokeRole;

grantTable: GRANT privilegeList ON TABLE? tableName TO roleList SEMICOLON?;
revokeTable: REVOKE privilegeList ON TABLE? tableName FROM roleList SEMICOLON?;

grantDatabase: GRANT dbPrivilegeList ON DATABASE databaseName TO roleList SEMICOLON?;
revokeDatabase: REVOKE dbPrivilegeList ON DATABASE databaseName FROM roleList SEMICOLON?;

grantRole: GRANT roleList TO roleList (WITH ADMIN OPTION)? SEMICOLON?;
revokeRole: REVOKE (ADMIN OPTION FOR)? roleList FROM roleList SEMICOLON?;

roleList: roleName (COMMA roleName)*;
dbPrivilegeList: dbPrivilegeName (COMMA dbPrivilegeName)*;
dbPrivilegeName: CONNECT | CREATE | TEMP | TEMPORARY | ALL PRIVILEGES?;
privilegeList: privilegeName (COMMA privilegeName)*;
privilegeName: SELECT | INSERT | UPDATE | DELETE | ALL PRIVILEGES?;
roleName: anyIdentifier;
alterTableAddColumn: ALTER TABLE tableName ADD COLUMN? columnName dataType (DEFAULT defaultValue)? SEMICOLON?;
alterTableDropColumn: ALTER TABLE tableName DROP COLUMN? columnName SEMICOLON?;
alterTableRenameColumn: ALTER TABLE tableName RENAME COLUMN columnName TO columnName SEMICOLON?;
alterTableRenameTable: ALTER TABLE tableName RENAME TO tableName SEMICOLON?;
alterTableAlterColumnType: ALTER TABLE tableName ALTER COLUMN? columnName TYPE dataType SEMICOLON?;
alterTableSetDefault: ALTER TABLE tableName ALTER COLUMN? columnName SET DEFAULT defaultValue SEMICOLON?;
alterTableDropDefault: ALTER TABLE tableName ALTER COLUMN? columnName DROP DEFAULT SEMICOLON?;
alterTableAddConstraint: ALTER TABLE tableName ADD tableConstraint SEMICOLON?;
alterTableDropConstraint: ALTER TABLE tableName DROP CONSTRAINT (PLSQL_IF EXISTS)? constraintName SEMICOLON?;
alterTableEnableRls: ALTER TABLE tableName ENABLE ROW LEVEL SECURITY SEMICOLON?;
alterTableDisableRls: ALTER TABLE tableName DISABLE ROW LEVEL SECURITY SEMICOLON?;
alterTableForceRls: ALTER TABLE tableName FORCE ROW LEVEL SECURITY SEMICOLON?;

createPolicy: CREATE POLICY policyName ON tableName (FOR policyCommand)? (TO roleName)?
              USING LPAREN expression RPAREN (WITH CHECK LPAREN expression RPAREN)? SEMICOLON?;
dropPolicy: DROP POLICY policyName ON tableName SEMICOLON?;
policyCommand: SELECT | INSERT | UPDATE | DELETE | ALL;
policyName: anyIdentifier;
createView: CREATE VIEW viewName AS select SEMICOLON?;
dropView: DROP VIEW viewName SEMICOLON?;
viewName: anyIdentifier;
showTables: SHOW TABLES SEMICOLON?;
showStats: SHOW STATS SEMICOLON?;
showTableStats: SHOW TABLE STATS SEMICOLON?;
showTransactionIsolationLevel: SHOW TRANSACTION ISOLATION LEVEL SEMICOLON?;
showStatements: SHOW STATEMENTS SEMICOLON?;
showActivity: SHOW ACTIVITY SEMICOLON?;
showParameter: SHOW IDENTIFIER SEMICOLON?;
setParameter: SET IDENTIFIER ASSIGN (literal | IDENTIFIER) SEMICOLON?;
showCatalog: SHOW CATALOG SEMICOLON?;
createSequence: CREATE SEQUENCE (PLSQL_IF NOT EXISTS)? sequenceName (AS dataType)? sequenceOption* SEMICOLON?;
alterSequence: ALTER SEQUENCE (PLSQL_IF EXISTS)? sequenceName (AS dataType)? alterSequenceOption* SEMICOLON?;
createType: CREATE TYPE typeName AS ENUM LPAREN STRING_LITERAL (COMMA STRING_LITERAL)* RPAREN SEMICOLON?;
dropSequence: DROP SEQUENCE (PLSQL_IF EXISTS)? sequenceName SEMICOLON?;
dropType: DROP TYPE typeName SEMICOLON?;
sequenceName: (anyIdentifier DOT)? anyIdentifier;
sequenceOption: START WITH? INTEGER_LITERAL
              | INCREMENT BY? INTEGER_LITERAL
              | MINVALUE INTEGER_LITERAL
              | NO MINVALUE
              | MAXVALUE INTEGER_LITERAL
              | NO MAXVALUE
              | CACHE INTEGER_LITERAL
              | CYCLE
              | NO CYCLE
              | OWNED BY (NONE | columnName)
              ;
alterSequenceOption: sequenceOption
                   | RESTART (WITH? INTEGER_LITERAL)?
                   ;
createFunction: CREATE (OR REPLACE)? FUNCTION functionName LPAREN (functionParam (COMMA functionParam)*)? RPAREN RETURNS dataType AS DOLLAR_QUOTED_STRING LANGUAGE (SQL_LANG | IDENTIFIER) SEMICOLON?;
dropFunction: DROP FUNCTION functionName SEMICOLON?;
functionName: IDENTIFIER (DOT IDENTIFIER)?;
createExtension: CREATE EXTENSION extensionName AS STRING_LITERAL SEMICOLON?;
dropExtension: DROP EXTENSION extensionName SEMICOLON?;
extensionName: IDENTIFIER;
createNativeFunction: CREATE (OR REPLACE)? FUNCTION functionName LPAREN (functionParam (COMMA functionParam)*)? RPAREN RETURNS dataType AS extensionName COMMA STRING_LITERAL LANGUAGE IDENTIFIER SEMICOLON?;
functionParam: IDENTIFIER dataType;
functionCall: functionName LPAREN (functionArg (COMMA functionArg)*)? RPAREN;
functionArg: literal | columnName;
createProcedure: CREATE (OR REPLACE)? PROCEDURE procedureName LPAREN (functionParam (COMMA functionParam)*)? RPAREN AS DOLLAR_QUOTED_STRING LANGUAGE (SQL_LANG | IDENTIFIER) SEMICOLON?;
dropProcedure: DROP PROCEDURE procedureName SEMICOLON?;
procedureName: anyIdentifier;
callStatement: CALL procedureName LPAREN (functionArg (COMMA functionArg)*)? RPAREN SEMICOLON?;
createTrigger: CREATE TRIGGER triggerName (BEFORE | AFTER) (INSERT | UPDATE | DELETE) ON tableName FOR EACH ROW EXECUTE (FUNCTION | PROCEDURE) triggerHandlerName LPAREN RPAREN SEMICOLON?;
dropTrigger: DROP TRIGGER triggerName ON tableName SEMICOLON?;
triggerName: anyIdentifier;
triggerHandlerName: IDENTIFIER;
explain: EXPLAIN select;
analyze: ANALYZE (TABLE? tableName)? SEMICOLON?;
vacuum: VACUUM tableName SEMICOLON?;
reindexStatement: REINDEX (TABLE tableName | INDEX indexName | tableName) SEMICOLON?;
checkpointStatement: CHECKPOINT SEMICOLON?;
promoteStatement: PROMOTE SEMICOLON?;
beginTxn: (BEGIN | START) TRANSACTION? (ISOLATION LEVEL isolationLevel | transactionMode)* SEMICOLON?;
setTransaction: SET (LOCAL | SESSION)? TRANSACTION (ISOLATION LEVEL isolationLevel | transactionMode)* SEMICOLON?;
setSessionCharacteristics: SET SESSION CHARACTERISTICS AS TRANSACTION (ISOLATION LEVEL isolationLevel | transactionMode)* SEMICOLON?;
isolationLevel: READ COMMITTED | REPEATABLE READ | SERIALIZABLE | READ UNCOMMITTED;
transactionMode: READ ONLY_KW | READ WRITE;
commitTxn: COMMIT TRANSACTION? SEMICOLON?;
rollbackTxn: ROLLBACK TRANSACTION? SEMICOLON?;
savepoint: SAVEPOINT savepointName SEMICOLON?;
releaseSavepoint: RELEASE SAVEPOINT? savepointName SEMICOLON?;
rollbackToSavepoint: ROLLBACK TO SAVEPOINT? savepointName SEMICOLON?;
savepointName: anyIdentifier;

// DML
insert: INSERT INTO tableName (LPAREN columnName (COMMA columnName)* RPAREN)? VALUES LPAREN valueList RPAREN (COMMA LPAREN valueList RPAREN)* returningClause? SEMICOLON?;
returningClause: RETURNING (STAR | columnName (COMMA columnName)*);
select: SELECT (ALL | DISTINCT)? selectList (FROM tableSource joinClause*)? (WHERE expression)? (GROUP BY groupByList)? (HAVING havingClause)? (ORDER BY orderList)? (LIMIT limitValue)? (OFFSET offsetValue)? forUpdateClause? SEMICOLON?;
forUpdateClause: FOR UPDATE (NOWAIT)?;
tableSource: (tableName | LPAREN select RPAREN) (AS? alias)?;
selectWithCte: WITH RECURSIVE? cteName AS LPAREN select (UNION ALL select)? RPAREN select;
cteName: anyIdentifier;
joinClause: (joinType)? JOIN tableSource ON LPAREN? columnName ASSIGN columnName RPAREN?;

joinType: INNER
        | LEFT OUTER?
        | RIGHT OUTER?
        | FULL OUTER?
        | CROSS
        | SEMI
        | ANTI
        ;
groupByList: columnName (COMMA columnName)*;
havingClause: aggregateFunction ASSIGN literal
            | aggregateFunction GT literal
            | aggregateFunction LT literal
            | aggregateFunction GE literal
            | aggregateFunction LE literal
            | aggregateFunction NE literal;
aggregateFunction: (COUNT | SUM | AVG | MIN | MAX) LPAREN (ALL | DISTINCT)? (STAR | columnName) RPAREN;
update: UPDATE tableName SET assignment (COMMA assignment)* (WHERE expression)? SEMICOLON?;
delete: DELETE FROM tableName (WHERE expression)? SEMICOLON?;

// Definitions
columnDef: columnName dataType columnConstraint*;
columnConstraint: NOT NULL
                | NULL
                | PRIMARY KEY
                | UNIQUE
                | DEFAULT defaultValue
                | (CONSTRAINT constraintName)? REFERENCES tableName (LPAREN columnName RPAREN)? fkAction*
                | (CONSTRAINT constraintName)? CHECK LPAREN expression RPAREN
                ;
assignment: columnName ASSIGN literal;
orderList: orderItem (COMMA orderItem)*;
orderItem: columnName (ASC | DESC)?;
indexName: anyIdentifier;

// Select list
selectList: STAR | selectItem (COMMA selectItem)*;
selectItem: windowFunction (AS alias)? | aggregateFunction (AS alias)? | functionCall (AS alias)? | literal (AS alias)? | expression (AS alias)? | columnName (AS alias)?;
windowFunction: (ROW_NUMBER | RANK | DENSE_RANK) LPAREN RPAREN OVER LPAREN (PARTITION BY groupByList)? (ORDER BY orderList)? RPAREN;

// Expressions
// Alternative order matters for ANTLR4's left-recursion precedence: earlier
// alternatives bind tighter. Comparisons/IN/EXISTS are the atoms; NOT binds
// tighter than AND, which binds tighter than OR - standard SQL precedence.
// Labels (#Name) give each alternative its own generated context class,
// making SqlParser.buildWhereExpr's tree-walk a straightforward instanceof
// switch instead of manually checking which optional token is non-null.
expression: LPAREN expression RPAREN                              #ParenExpr
          | NOT expression                                        #NotExpr
          | columnName ASSIGN columnName                           #EqColumnCompare
          | columnName GT columnName                                #GtColumnCompare
          | columnName LT columnName                                #LtColumnCompare
          | columnName GE columnName                                #GeColumnCompare
          | columnName LE columnName                                #LeColumnCompare
          | columnName NE columnName                                #NeColumnCompare
          | columnName ASSIGN literal                              #EqCompare
          | columnName ASSIGN IDENTIFIER LPAREN RPAREN             #EqZeroArgFunctionCompare
          | columnName GT literal                                  #GtCompare
          | columnName LT literal                                  #LtCompare
          | columnName GE literal                                  #GeCompare
          | columnName LE literal                                  #LeCompare
          | columnName NE literal                                  #NeCompare
          | columnName BETWEEN literal AND literal                  #BetweenCompare
          | columnName NOT BETWEEN literal AND literal              #NotBetweenCompare
          | columnName LIKE literal                                #LikeCompare
          | columnName CONTAINS literal                            #ContainsCompare
          | columnName ARRAY_CONTAINS literal                      #ArrayContainsCompare
          | columnName TS_MATCH literal                           #TsMatchCompare
          | columnName JSON_EXTRACT_TEXT literal ASSIGN literal    #JsonExtractTextEqCompare
          | LPAREN columnName COMMA columnName RPAREN OVERLAPS LPAREN literal COMMA literal RPAREN #RangeOverlapsCompare
          | columnName IN LPAREN valueList RPAREN                  #InListExpr
          | columnName NOT IN LPAREN valueList RPAREN               #NotInListExpr
          | columnName IN LPAREN select RPAREN                      #InSubqueryExpr
          | columnName NOT IN LPAREN select RPAREN                  #NotInSubqueryExpr
          | columnName ASSIGN LPAREN select RPAREN                  #EqSubqueryExpr
          | columnName GT LPAREN select RPAREN                      #GtSubqueryExpr
          | columnName LT LPAREN select RPAREN                      #LtSubqueryExpr
          | columnName GE LPAREN select RPAREN                      #GeSubqueryExpr
          | columnName LE LPAREN select RPAREN                      #LeSubqueryExpr
          | columnName NE LPAREN select RPAREN                      #NeSubqueryExpr
          | EXISTS LPAREN select RPAREN                              #ExistsExpr
          | NOT EXISTS LPAREN select RPAREN                          #NotExistsExpr
          | expression AND expression                                #AndExpr
          | expression OR expression                                 #OrExpr
          ;

// Values
valueList: insertValue (COMMA insertValue)*;
insertValue: literal | NEXTVAL LPAREN STRING_LITERAL RPAREN | CURRVAL LPAREN STRING_LITERAL RPAREN | arrayLiteral;
arrayLiteral: ARRAY LBRACKET (literal (COMMA literal)*)? RBRACKET;

// Data types
dataType: (INT | INTEGER | BIGINT | SMALLINT | TINYINT | SERIAL | BIGSERIAL
        | INT2 | INT4 | INT8
        | FLOAT4 | FLOAT8
        | VARCHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | CHARACTER VARYING (LPAREN INTEGER_LITERAL RPAREN)?
        | TEXT | CHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | BOOLEAN | BOOL
        | DATE
        | TIME (LPAREN INTEGER_LITERAL RPAREN)? (WITH TIME ZONE | WITHOUT TIME ZONE)?
        | TIMESTAMP (LPAREN INTEGER_LITERAL RPAREN)? (WITH TIME ZONE | WITHOUT TIME ZONE)?
        | TIMESTAMPTZ
        | DECIMAL (LPAREN INTEGER_LITERAL (COMMA INTEGER_LITERAL)? RPAREN)?
        | NUMERIC (LPAREN INTEGER_LITERAL (COMMA INTEGER_LITERAL)? RPAREN)?
        | DOUBLE PRECISION? | FLOAT | REAL
        | BYTEA | BLOB
        | UUID
        | JSON | JSONB
        | INET | CIDR
        | INT4RANGE | DATERANGE
        | TSVECTOR | TSQUERY
        // A bare IDENTIFIER names a real, user-defined type (see createType's
        // own grammar rule) - an enum created via CREATE TYPE ... AS ENUM,
        // referenced here by its own real name (e.g. "status mood_enum") the
        // same way any of this rule's own built-in keywords are. Listed last
        // so a real, hardcoded type keyword above is always preferred over
        // this generic fallback where both could otherwise match.
        | IDENTIFIER) (LBRACKET RBRACKET)?;

// Literals
literal: STRING_LITERAL | MINUS? INTEGER_LITERAL | MINUS? FLOAT_LITERAL | TRUE | FALSE | NULL | PARAMETER;
defaultValue: literal
            | CURRENT_DATE
            | CURRENT_TIME
            | CURRENT_TIMESTAMP
            | NEXTVAL LPAREN STRING_LITERAL (COLONCOLON anyIdentifier)? RPAREN
            | anyIdentifier LPAREN (literal | anyIdentifier | (STRING_LITERAL COLONCOLON anyIdentifier))? RPAREN
            ;

// Identifiers
anyIdentifier: IDENTIFIER
             | QUOTED_IDENTIFIER
             | ADMIN | CONNECT | OPTION | TEMP | TEMPORARY | TABLES | TYPE | VIEW | INDEX | DEFAULT
             | ROLE | KEY | ACTION | CASCADE | RESTRICT | PRIMARY | UNIQUE | CHECK
             | NONE | MINVALUE | MAXVALUE | CACHE | CYCLE | RESTART | OWNED | ZONE | WITHOUT | CHARACTER | VARYING
             | BETWEEN
             ;
tableName: (anyIdentifier DOT)? anyIdentifier;
typeName: anyIdentifier;
columnName: (anyIdentifier DOT (anyIdentifier DOT)?)? anyIdentifier;
alias: anyIdentifier | STRING_LITERAL;
limitValue: INTEGER_LITERAL | PARAMETER;
offsetValue: INTEGER_LITERAL | PARAMETER;

// Lexer rules
// TRUE/FALSE MUST be declared before IDENTIFIER (see IDENTIFIER's own
// comment below for why token declaration order matters here) - a real,
// previously-latent bug: these were originally declared far below,
// after IDENTIFIER, meaning ANTLR's lexer - which breaks a tie between
// two rules matching the exact same input length by picking whichever
// was declared FIRST - always tokenized a bare "true"/"false" as a
// generic IDENTIFIER, never as these two rules. `INSERT INTO t VALUES
// (true)` failed with a genuine syntax error; found while building
// COPY's own HEADER boolean option, but this affected every bare
// boolean literal anywhere in this SQL dialect, not just COPY.
//
// A second, related, real bug fixed alongside it: a separate
// BOOLEAN_LITERAL: TRUE | FALSE; lexer rule used to exist too, and every
// parser rule below referenced BOOLEAN_LITERAL rather than TRUE/FALSE
// directly - but a composite lexer rule like that can NEVER actually be
// produced as its own token type once TRUE/FALSE also exist as
// separate, standalone lexer rules matching the identical text: the
// same declaration-order tie-break above means TRUE/FALSE always claim
// that input first, so BOOLEAN_LITERAL's own token type was never once
// emitted by the lexer no matter where it was declared - removed
// entirely; every rule that needs a boolean literal now references
// (TRUE | FALSE) directly, the token types the lexer actually produces.
TRUE: T R U E;
FALSE: F A L S E;
CREATE: C R E A T E;
TABLE: T A B L E;
VIEW: V I E W;
AS: A S;
WITH: W I T H;
UNION: U N I O N;
ALL: A L L;
RECURSIVE: R E C U R S I V E;
DROP: D R O P;
COPY: C O P Y;
STDIN: S T D I N;
STDOUT: S T D O U T;
FORMAT: F O R M A T;
DELIMITER: D E L I M I T E R;
HEADER: H E A D E R;
CSV: C S V;
ROLE: R O L E;
GRANT: G R A N T;
REVOKE: R E V O K E;
LOGIN: L O G I N;
NOLOGIN: N O L O G I N;
SUPERUSER: S U P E R U S E R;
NOSUPERUSER: N O S U P E R U S E R;
PRIVILEGES: P R I V I L E G E S;
CONNECT: C O N N E C T;
TEMP: T E M P;
TEMPORARY: T E M P O R A R Y;
ADMIN: A D M I N;
OPTION: O P T I O N;
PASSWORD: P A S S W O R D;
ALTER: A L T E R;
SECURITY: S E C U R I T Y;
ENABLE: E N A B L E;
DISABLE: D I S A B L E;
FORCE: F O R C E;
POLICY: P O L I C Y;
CHECK: C H E C K;
ADD: A D D;
COLUMN: C O L U M N;
RENAME: R E N A M E;
TYPE: T Y P E;
ENUM: E N U M;
INET: I N E T;
CIDR: C I D R;
INT4RANGE: I N T '4' R A N G E;
DATERANGE: D A T E R A N G E;
TSVECTOR: T S V E C T O R;
TSQUERY: T S Q U E R Y;
INSERT: I N S E R T;
INTO: I N T O;
VALUES: V A L U E S;
SELECT: S E L E C T;
FROM: F R O M;
WHERE: W H E R E;
BETWEEN: B E T W E E N;
UPDATE: U P D A T E;
SET: S E T;
DELETE: D E L E T E;
ORDER: O R D E R;
BY: B Y;
GROUP: G R O U P;
HAVING: H A V I N G;
LIMIT: L I M I T;
OFFSET: O F F S E T;
DATABASE: D A T A B A S E;
DATABASES: D A T A B A S E S;
ASC: A S C;
DESC: D E S C;
AND: A N D;
OR: O R;
NOT: N O T;
IN: I N;
EXISTS: E X I S T S;
LIKE: L I K E;
IS: I S;
NULL: N U L L;
DEFAULT: D E F A U L T;
SHOW: S H O W;
TABLES: T A B L E S;
STATS: S T A T S;
STATEMENTS: S T A T E M E N T S;
ACTIVITY: A C T I V I T Y;
CATALOG: C A T A L O G;
SEQUENCE: S E Q U E N C E;
FUNCTION: F U N C T I O N;
PROCEDURE: P R O C E D U R E;
CALL: C A L L;
TRIGGER: T R I G G E R;
BEFORE: B E F O R E;
AFTER: A F T E R;
FOR: F O R;
EACH: E A C H;
ROW: R O W;
EXECUTE: E X E C U T E;
EXTENSION: E X T E N S I O N;
RETURNS: R E T U R N S;
// --- Real procedural language (see plpgsqlBlock's own grammar rule and
// PlpgsqlInterpreter's own javadoc) - a genuinely separate, second parse
// entry point using this same lexer, invoked only when a function/procedure
// body is actually written in this procedural sub-language (LANGUAGE
// plpgsql), never mixed into ordinary top-level SQL statement parsing.
PLSQL_DECLARE: D E C L A R E;
PLSQL_END: E N D;
PLSQL_IF: I F;
PLSQL_THEN: T H E N;
PLSQL_ELSE: E L S E;
PLSQL_ELSIF: E L S I F;
PLSQL_WHILE: W H I L E;
PLSQL_LOOP: L O O P;
PLSQL_EXIT: E X I T;
PLSQL_CONTINUE: C O N T I N U E;
PLSQL_RETURN: R E T U R N;
PLSQL_RAISE: R A I S E;
PLSQL_NOTICE: N O T I C E;
PLSQL_EXCEPTION: E X C E P T I O N;
PLSQL_WARNING: W A R N I N G;
PLSQL_WHEN: W H E N;
PLUS: '+';
MINUS: '-';
DIVIDE: '/';
ASSIGN_PLSQL: ':=';
DOTDOT: '..';
LANGUAGE: L A N G U A G E;
REPLACE: R E P L A C E;
SQL_LANG: S Q L;
ROW_NUMBER: R O W '_' N U M B E R;
RANK: R A N K;
DENSE_RANK: D E N S E '_' R A N K;
OVER: O V E R;
PARTITION: P A R T I T I O N;
INCREMENT: I N C R E M E N T;
NEXTVAL: N E X T V A L;
CURRVAL: C U R R V A L;
SERIAL: S E R I A L;
BIGSERIAL: B I G S E R I A L;
INDEX: I N D E X;
ON: O N;
USING: U S I N G;
HASH: H A S H;
BTREE: B T R E E;
BRIN: B R I N;
GIN: G I N;
BITMAP: B I T M A P;
GIST: G I S T;
OVERLAPS: O V E R L A P S;
CONTAINS: C O N T A I N S;
JOIN: J O I N;
INNER: I N N E R;
LEFT: L E F T;
RIGHT: R I G H T;
FULL: F U L L;
OUTER: O U T E R;
CROSS: C R O S S;
SEMI: S E M I;
ANTI: A N T I;
EXPLAIN: E X P L A I N;
ANALYZE: A N A L Y Z E;
VACUUM: V A C U U M;
REINDEX: R E I N D E X;
CHECKPOINT: C H E C K P O I N T;
PROMOTE: P R O M O T E;
BEGIN: B E G I N;
START: S T A R T;
TRANSACTION: T R A N S A C T I O N;
PRIMARY: P R I M A R Y;
UNIQUE: U N I Q U E;
CONSTRAINT: C O N S T R A I N T;
FOREIGN: F O R E I G N;
REFERENCES: R E F E R E N C E S;
CASCADE: C A S C A D E;
RESTRICT: R E S T R I C T;
NO: N O;
ACTION: A C T I O N;
RETURNING: R E T U R N I N G;
KEY: K E Y;
ISOLATION: I S O L A T I O N;
LEVEL: L E V E L;
READ: R E A D;
WRITE: W R I T E;
COMMITTED: C O M M I T T E D;
UNCOMMITTED: U N C O M M I T T E D;
REPEATABLE: R E P E A T A B L E;
SERIALIZABLE: S E R I A L I Z A B L E;
CHARACTERISTICS: C H A R A C T E R I S T I C S;
LOCAL: L O C A L;
SESSION: S E S S I O N;
ONLY_KW: O N L Y;
NOWAIT: N O W A I T;
COMMIT: C O M M I T;
ROLLBACK: R O L L B A C K;
SAVEPOINT: S A V E P O I N T;
RELEASE: R E L E A S E;
TO: T O;
DISTINCT: D I S T I N C T;
MINVALUE: M I N V A L U E;
MAXVALUE: M A X V A L U E;
CACHE: C A C H E;
CYCLE: C Y C L E;
RESTART: R E S T A R T;
OWNED: O W N E D;
ZONE: Z O N E;
WITHOUT: W I T H O U T;
CHARACTER: C H A R A C T E R;
VARYING: V A R Y I N G;
NONE: N O N E;

COUNT: C O U N T;
SUM: S U M;
AVG: A V G;
MIN: M I N;
MAX: M A X;

// Data type keywords
INT: I N T;
INTEGER: I N T E G E R;
BIGINT: B I G I N T;
SMALLINT: S M A L L I N T;
TINYINT: T I N Y I N T;
VARCHAR: V A R C H A R;
TEXT: T E X T;
CHAR: C H A R;
BOOLEAN: B O O L E A N;
BOOL: B O O L;
DATE: D A T E;
TIME: T I M E;
TIMESTAMP: T I M E S T A M P;
TIMESTAMPTZ: T I M E S T A M P T Z;
INT2: I N T '2';
INT4: I N T '4';
INT8: I N T '8';
FLOAT4: F L O A T '4';
FLOAT8: F L O A T '8';
DECIMAL: D E C I M A L;
NUMERIC: N U M E R I C;
DOUBLE: D O U B L E;
PRECISION: P R E C I S I O N;
FLOAT: F L O A T;
REAL: R E A L;
BYTEA: B Y T E A;
BLOB: B L O B;
UUID: U U I D;
ARRAY: A R R A Y;
LBRACKET: '[';
RBRACKET: ']';
JSON: J S O N;
JSONB: J S O N B;

CURRENT_DATE: C U R R E N T '_' D A T E;
CURRENT_TIME: C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;

// Operators and symbols
LPAREN: '(';
DOT: '.';
RPAREN: ')';
COMMA: ',';
SEMICOLON: ';';
ASSIGN: '=';
GT: '>';
LT: '<';
GE: '>=';
LE: '<=';
NE: '!=';
ARRAY_CONTAINS: '@>';
TS_MATCH: '@@';
JSON_EXTRACT_TEXT: '->>';
COLONCOLON: '::';
STAR: '*';

// Literals
// Any keyword-like token that could match the exact same text as a valid
// identifier (TRUE/FALSE being the real example that was actually
// bitten by this) MUST be declared before this rule - ANTLR's lexer
// breaks a tie between two rules matching the same-length input by
// picking whichever rule was declared first, so a keyword declared
// AFTER this one would always lose to being tokenized as a plain
// IDENTIFIER instead, no matter how the keyword's own rule is written.
QUOTED_IDENTIFIER: '"' ('""' | ~["])* '"';
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;
STRING_LITERAL: '\'' ('\'\'' | ~['])* '\'';
/** Postgres's own $$ ... $$ delimiter for a function body - lets the body contain semicolons, quotes, or anything else without conflicting with the surrounding statement's own delimiters. Non-greedy (.*?) so the FIRST closing $$ ends the body, not the last one in the whole remaining input. */
DOLLAR_QUOTED_STRING: '$$' .*? '$$';
PARAMETER: '$' [0-9]+ | '?';
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;

// Fragment rules for case-insensitivity
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

// Skip whitespace
WS: [ \t\r\n]+ -> skip;
LINE_COMMENT: '--' ~[\r\n]* -> skip;
BLOCK_COMMENT: '/*' .*? '*/' -> skip;