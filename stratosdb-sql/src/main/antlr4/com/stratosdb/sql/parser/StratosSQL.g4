grammar StratosSQL;

// Parser rules
parse: sqlStatement EOF;

sqlStatement: createTable | createIndex | insert | selectWithCte | select | update | delete | dropTable | showTables | showStats | showCatalog | explain | analyze | vacuum | beginTxn | commitTxn | rollbackTxn | createView | dropView | savepoint | releaseSavepoint | rollbackToSavepoint | createSequence | dropSequence | createFunction | dropFunction | createProcedure | dropProcedure | callStatement | createTrigger | dropTrigger | createExtension | dropExtension | createNativeFunction | alterTableAddColumn | alterTableDropColumn | alterTableRenameColumn | alterTableRenameTable | alterTableAlterColumnType | alterTableSetDefault | alterTableDropDefault;

// DDL
createTable: CREATE TABLE tableName LPAREN columnDef (COMMA columnDef)* RPAREN SEMICOLON?;
createIndex: CREATE INDEX indexName ON tableName LPAREN columnName (COMMA columnName)? RPAREN (USING (HASH | BTREE | BRIN | GIN | BITMAP | GIST))? SEMICOLON?;
dropTable: DROP TABLE tableName SEMICOLON?;
alterTableAddColumn: ALTER TABLE tableName ADD COLUMN? columnName dataType (DEFAULT defaultValue)? SEMICOLON?;
alterTableDropColumn: ALTER TABLE tableName DROP COLUMN? columnName SEMICOLON?;
alterTableRenameColumn: ALTER TABLE tableName RENAME COLUMN columnName TO columnName SEMICOLON?;
alterTableRenameTable: ALTER TABLE tableName RENAME TO tableName SEMICOLON?;
alterTableAlterColumnType: ALTER TABLE tableName ALTER COLUMN? columnName TYPE dataType SEMICOLON?;
alterTableSetDefault: ALTER TABLE tableName ALTER COLUMN? columnName SET DEFAULT defaultValue SEMICOLON?;
alterTableDropDefault: ALTER TABLE tableName ALTER COLUMN? columnName DROP DEFAULT SEMICOLON?;
createView: CREATE VIEW viewName AS select SEMICOLON?;
dropView: DROP VIEW viewName SEMICOLON?;
viewName: IDENTIFIER;
showTables: SHOW TABLES SEMICOLON?;
showStats: SHOW STATS SEMICOLON?;
showCatalog: SHOW CATALOG SEMICOLON?;
createSequence: CREATE SEQUENCE sequenceName (START WITH? INTEGER_LITERAL)? (INCREMENT BY? INTEGER_LITERAL)? SEMICOLON?;
dropSequence: DROP SEQUENCE sequenceName SEMICOLON?;
sequenceName: IDENTIFIER;
createFunction: CREATE (OR REPLACE)? FUNCTION functionName LPAREN (functionParam (COMMA functionParam)*)? RPAREN RETURNS dataType AS DOLLAR_QUOTED_STRING LANGUAGE (SQL_LANG | IDENTIFIER) SEMICOLON?;
dropFunction: DROP FUNCTION functionName SEMICOLON?;
functionName: IDENTIFIER;
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
beginTxn: BEGIN TRANSACTION? SEMICOLON? | START TRANSACTION SEMICOLON?;
commitTxn: COMMIT SEMICOLON?;
rollbackTxn: ROLLBACK SEMICOLON?;
savepoint: SAVEPOINT savepointName SEMICOLON?;
releaseSavepoint: RELEASE SAVEPOINT? savepointName SEMICOLON?;
rollbackToSavepoint: ROLLBACK TO SAVEPOINT? savepointName SEMICOLON?;
savepointName: IDENTIFIER;

// DML
insert: INSERT INTO tableName (LPAREN columnName (COMMA columnName)* RPAREN)? VALUES LPAREN valueList RPAREN SEMICOLON?;
select: SELECT selectList FROM tableName joinClause* (WHERE expression)? (GROUP BY groupByList)? (HAVING havingClause)? (ORDER BY orderList)? (LIMIT limitValue)? SEMICOLON?;
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
columnDef: columnName dataType (NOT NULL)? (DEFAULT defaultValue)?;
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
          | columnName GT literal                                  #GtCompare
          | columnName LT literal                                  #LtCompare
          | columnName GE literal                                  #GeCompare
          | columnName LE literal                                  #LeCompare
          | columnName NE literal                                  #NeCompare
          | columnName LIKE literal                                #LikeCompare
          | columnName CONTAINS literal                            #ContainsCompare
          | columnName ARRAY_CONTAINS literal                      #ArrayContainsCompare
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
        | JSON | JSONB) (LBRACKET RBRACKET)?;

// Literals
literal: STRING_LITERAL | INTEGER_LITERAL | FLOAT_LITERAL | BOOLEAN_LITERAL | NULL;
defaultValue: literal | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | NEXTVAL LPAREN STRING_LITERAL RPAREN;

// Identifiers
tableName: IDENTIFIER;
columnName: IDENTIFIER (DOT IDENTIFIER)?;
alias: IDENTIFIER | STRING_LITERAL;
limitValue: INTEGER_LITERAL;

// Lexer rules
CREATE: C R E A T E;
TABLE: T A B L E;
VIEW: V I E W;
AS: A S;
WITH: W I T H;
UNION: U N I O N;
ALL: A L L;
RECURSIVE: R E C U R S I V E;
DROP: D R O P;
ALTER: A L T E R;
ADD: A D D;
COLUMN: C O L U M N;
RENAME: R E N A M E;
TYPE: T Y P E;
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
BEGIN: B E G I N;
START: S T A R T;
TRANSACTION: T R A N S A C T I O N;
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
JSON_EXTRACT_TEXT: '->>';
STAR: '*';

// Literals
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;
STRING_LITERAL: '\'' ('\'\'' | ~['])* '\'';
/** Postgres's own $$ ... $$ delimiter for a function body - lets the body contain semicolons, quotes, or anything else without conflicting with the surrounding statement's own delimiters. Non-greedy (.*?) so the FIRST closing $$ ends the body, not the last one in the whole remaining input. */
DOLLAR_QUOTED_STRING: '$$' .*? '$$';
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;
BOOLEAN_LITERAL: TRUE | FALSE;

// Boolean literals (must be defined as lexer tokens)
TRUE: T R U E;
FALSE: F A L S E;

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