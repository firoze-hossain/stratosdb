grammar StratosSQL;

// Parser rules
parse: sqlStatement EOF;

sqlStatement: createTable | createIndex | insert | selectWithCte | select | update | delete | dropTable | showTables | showStats | showTableStats | showStatements | showActivity | showTransactionIsolationLevel | showParameter | setParameter | showCatalog | explain | analyze | vacuum | beginTxn | commitTxn | rollbackTxn | createView | dropView | savepoint | releaseSavepoint | rollbackToSavepoint | createSequence | dropSequence | createType | dropType | createFunction | dropFunction | createProcedure | dropProcedure | callStatement | createTrigger | dropTrigger | createExtension | dropExtension | createNativeFunction | alterTableAddColumn | alterTableDropColumn | alterTableRenameColumn | alterTableRenameTable | alterTableAlterColumnType | alterTableSetDefault | alterTableDropDefault | alterTableEnableRls | alterTableDisableRls | alterTableForceRls | createPolicy | dropPolicy | createRole | dropRole | grantStatement | revokeStatement | copyStatement | checkpointStatement | promoteStatement | createDatabase | dropDatabase | showDatabases;

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
createTable: CREATE TABLE tableName LPAREN columnDef (COMMA columnDef)* (COMMA PRIMARY KEY LPAREN columnName (COMMA columnName)* RPAREN)? RPAREN SEMICOLON?;
createIndex: CREATE INDEX indexName ON tableName LPAREN columnName (COMMA columnName)? RPAREN (USING (HASH | BTREE | BRIN | GIN | BITMAP | GIST))? SEMICOLON?;
dropTable: DROP TABLE tableName SEMICOLON?;
createDatabase: CREATE DATABASE databaseName SEMICOLON?;
dropDatabase: DROP DATABASE databaseName SEMICOLON?;
showDatabases: SHOW DATABASES SEMICOLON?;
databaseName: IDENTIFIER;
copyStatement: COPY tableName (LPAREN columnName (COMMA columnName)* RPAREN)? (FROM | TO) copyTarget (WITH? LPAREN copyOption (COMMA copyOption)* RPAREN)? SEMICOLON?;
copyTarget: STRING_LITERAL | STDIN | STDOUT;
copyOption: FORMAT (TEXT | CSV) | DELIMITER STRING_LITERAL | HEADER (TRUE | FALSE)? | NULL STRING_LITERAL;
createRole: CREATE ROLE roleName (WITH)? roleOption* SEMICOLON?;
roleOption: LOGIN | NOLOGIN | SUPERUSER | NOSUPERUSER | PASSWORD STRING_LITERAL;
dropRole: DROP ROLE roleName SEMICOLON?;
grantStatement: GRANT privilegeList ON TABLE? tableName TO roleName SEMICOLON?;
revokeStatement: REVOKE privilegeList ON TABLE? tableName FROM roleName SEMICOLON?;
privilegeList: privilegeName (COMMA privilegeName)*;
privilegeName: SELECT | INSERT | UPDATE | DELETE | ALL PRIVILEGES?;
roleName: IDENTIFIER;
alterTableAddColumn: ALTER TABLE tableName ADD COLUMN? columnName dataType (DEFAULT defaultValue)? SEMICOLON?;
alterTableDropColumn: ALTER TABLE tableName DROP COLUMN? columnName SEMICOLON?;
alterTableRenameColumn: ALTER TABLE tableName RENAME COLUMN columnName TO columnName SEMICOLON?;
alterTableRenameTable: ALTER TABLE tableName RENAME TO tableName SEMICOLON?;
alterTableAlterColumnType: ALTER TABLE tableName ALTER COLUMN? columnName TYPE dataType SEMICOLON?;
alterTableSetDefault: ALTER TABLE tableName ALTER COLUMN? columnName SET DEFAULT defaultValue SEMICOLON?;
alterTableDropDefault: ALTER TABLE tableName ALTER COLUMN? columnName DROP DEFAULT SEMICOLON?;
alterTableEnableRls: ALTER TABLE tableName ENABLE ROW LEVEL SECURITY SEMICOLON?;
alterTableDisableRls: ALTER TABLE tableName DISABLE ROW LEVEL SECURITY SEMICOLON?;
alterTableForceRls: ALTER TABLE tableName FORCE ROW LEVEL SECURITY SEMICOLON?;

createPolicy: CREATE POLICY policyName ON tableName (FOR policyCommand)? (TO roleName)?
              USING LPAREN expression RPAREN (WITH CHECK LPAREN expression RPAREN)? SEMICOLON?;
dropPolicy: DROP POLICY policyName ON tableName SEMICOLON?;
policyCommand: SELECT | INSERT | UPDATE | DELETE | ALL;
policyName: IDENTIFIER;
createView: CREATE VIEW viewName AS select SEMICOLON?;
dropView: DROP VIEW viewName SEMICOLON?;
viewName: IDENTIFIER;
showTables: SHOW TABLES SEMICOLON?;
showStats: SHOW STATS SEMICOLON?;
showTableStats: SHOW TABLE STATS SEMICOLON?;
showTransactionIsolationLevel: SHOW TRANSACTION ISOLATION LEVEL SEMICOLON?;
showStatements: SHOW STATEMENTS SEMICOLON?;
showActivity: SHOW ACTIVITY SEMICOLON?;
showParameter: SHOW IDENTIFIER SEMICOLON?;
setParameter: SET IDENTIFIER ASSIGN (literal | IDENTIFIER) SEMICOLON?;
showCatalog: SHOW CATALOG SEMICOLON?;
createSequence: CREATE SEQUENCE sequenceName (START WITH? INTEGER_LITERAL)? (INCREMENT BY? INTEGER_LITERAL)? SEMICOLON?;
createType: CREATE TYPE typeName AS ENUM LPAREN STRING_LITERAL (COMMA STRING_LITERAL)* RPAREN SEMICOLON?;
dropSequence: DROP SEQUENCE sequenceName SEMICOLON?;
dropType: DROP TYPE typeName SEMICOLON?;
sequenceName: IDENTIFIER;
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
procedureName: IDENTIFIER;
callStatement: CALL procedureName LPAREN (functionArg (COMMA functionArg)*)? RPAREN SEMICOLON?;
createTrigger: CREATE TRIGGER triggerName (BEFORE | AFTER) (INSERT | UPDATE | DELETE) ON tableName FOR EACH ROW EXECUTE (FUNCTION | PROCEDURE) triggerHandlerName LPAREN RPAREN SEMICOLON?;
dropTrigger: DROP TRIGGER triggerName ON tableName SEMICOLON?;
triggerName: IDENTIFIER;
triggerHandlerName: IDENTIFIER;
explain: EXPLAIN select;
analyze: ANALYZE tableName SEMICOLON?;
vacuum: VACUUM tableName SEMICOLON?;
checkpointStatement: CHECKPOINT SEMICOLON?;
promoteStatement: PROMOTE SEMICOLON?;
beginTxn: BEGIN TRANSACTION? SEMICOLON? | START TRANSACTION SEMICOLON?;
commitTxn: COMMIT SEMICOLON?;
rollbackTxn: ROLLBACK SEMICOLON?;
savepoint: SAVEPOINT savepointName SEMICOLON?;
releaseSavepoint: RELEASE SAVEPOINT? savepointName SEMICOLON?;
rollbackToSavepoint: ROLLBACK TO SAVEPOINT? savepointName SEMICOLON?;
savepointName: IDENTIFIER;

// DML
insert: INSERT INTO tableName (LPAREN columnName (COMMA columnName)* RPAREN)? VALUES LPAREN valueList RPAREN (COMMA LPAREN valueList RPAREN)* returningClause? SEMICOLON?;
returningClause: RETURNING (STAR | columnName (COMMA columnName)*);
select: SELECT selectList (FROM tableName (AS? IDENTIFIER)? joinClause*)? (WHERE expression)? (GROUP BY groupByList)? (HAVING havingClause)? (ORDER BY orderList)? (LIMIT limitValue)? (OFFSET offsetValue)? SEMICOLON?;
selectWithCte: WITH RECURSIVE? cteName AS LPAREN select (UNION ALL select)? RPAREN select;
cteName: IDENTIFIER;
joinClause: (INNER)? JOIN tableName ON columnName ASSIGN columnName;
groupByList: columnName (COMMA columnName)*;
havingClause: aggregateFunction ASSIGN literal
            | aggregateFunction GT literal
            | aggregateFunction LT literal
            | aggregateFunction GE literal
            | aggregateFunction LE literal
            | aggregateFunction NE literal;
aggregateFunction: (COUNT | SUM | AVG | MIN | MAX) LPAREN (STAR | columnName) RPAREN;
update: UPDATE tableName SET assignment (COMMA assignment)* (WHERE expression)? SEMICOLON?;
delete: DELETE FROM tableName (WHERE expression)? SEMICOLON?;

// Definitions
columnDef: columnName dataType (NOT NULL)? (DEFAULT defaultValue)? (PRIMARY KEY)?;
assignment: columnName ASSIGN literal;
orderList: orderItem (COMMA orderItem)*;
orderItem: columnName (ASC | DESC)?;
indexName: IDENTIFIER;

// Select list
selectList: STAR | selectItem (COMMA selectItem)*;
selectItem: windowFunction (AS alias)? | aggregateFunction (AS alias)? | functionCall (AS alias)? | expression (AS alias)? | columnName (AS alias)?;
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
        | VARCHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | TEXT | CHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | BOOLEAN | BOOL
        | DATE | TIME | TIMESTAMP
        | DECIMAL LPAREN INTEGER_LITERAL COMMA INTEGER_LITERAL RPAREN
        | DOUBLE | FLOAT
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
literal: STRING_LITERAL | MINUS? INTEGER_LITERAL | MINUS? FLOAT_LITERAL | TRUE | FALSE | NULL;
defaultValue: literal | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | NEXTVAL LPAREN STRING_LITERAL RPAREN;

// Identifiers
tableName: IDENTIFIER;
typeName: IDENTIFIER;
columnName: IDENTIFIER (DOT IDENTIFIER)?;
alias: IDENTIFIER | STRING_LITERAL;
limitValue: INTEGER_LITERAL;
offsetValue: INTEGER_LITERAL;

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
EXPLAIN: E X P L A I N;
ANALYZE: A N A L Y Z E;
VACUUM: V A C U U M;
CHECKPOINT: C H E C K P O I N T;
PROMOTE: P R O M O T E;
BEGIN: B E G I N;
START: S T A R T;
TRANSACTION: T R A N S A C T I O N;
PRIMARY: P R I M A R Y;
RETURNING: R E T U R N I N G;
KEY: K E Y;
ISOLATION: I S O L A T I O N;
LEVEL: L E V E L;
COMMIT: C O M M I T;
ROLLBACK: R O L L B A C K;
SAVEPOINT: S A V E P O I N T;
RELEASE: R E L E A S E;
TO: T O;

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
DECIMAL: D E C I M A L;
DOUBLE: D O U B L E;
FLOAT: F L O A T;
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
STAR: '*';

// Literals
// Any keyword-like token that could match the exact same text as a valid
// identifier (TRUE/FALSE being the real example that was actually
// bitten by this) MUST be declared before this rule - ANTLR's lexer
// breaks a tie between two rules matching the same-length input by
// picking whichever rule was declared first, so a keyword declared
// AFTER this one would always lose to being tokenized as a plain
// IDENTIFIER instead, no matter how the keyword's own rule is written.
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;
STRING_LITERAL: '\'' ('\'\'' | ~['])* '\'';
/** Postgres's own $$ ... $$ delimiter for a function body - lets the body contain semicolons, quotes, or anything else without conflicting with the surrounding statement's own delimiters. Non-greedy (.*?) so the FIRST closing $$ ends the body, not the last one in the whole remaining input. */
DOLLAR_QUOTED_STRING: '$$' .*? '$$';
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