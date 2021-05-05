unit uSQLBuilder;

interface

uses
    Classes, uSQLExpression, uDBConnection, Datasnap.DBClient,
    System.SysUtils, DBXJSON, System.Generics.Collections;

type
    TSQLBuilder = class
        private
            FDBConn : TDBConnection;
            FSelectFields : TStringList;
            FInsertFields : TStringList;
            FInsertValues : TStringList;
            FJoins: TStringList;
            FOrder : TStringList;
            FSQLExpression : TSQLExpression;
            FFirst : Integer;
            FSkip : Integer;
            FFrom : string;
            function buildSelect : String;
            function buildInsert : string;
            function buildUpdate : string;
            function buildDelete : string;
            function buildExecuteProcedure: string;
            procedure bindParam(field, value : string);
            procedure clear;
        public
            Constructor Create(dbUser, dbPassword, dbHost, dbName: string);
            destructor Destroy; override;
            function genId(generator: string; step : integer = 1) : TSQLBuilder;
            function fill(field, value : string) : TSQLBuilder; overload;
            function fill(field : string; value : currency) : TSQLBuilder; overload;
            function fill(field : string; value : boolean)  : TSQLBuilder; overload;
            function from(table : string) : TSQLBuilder;
            function select(field : string) : TSQLBuilder;
            function where(field, exp, value : string) : TSQLBuilder; overload;
            function where(field, value : string) : TSQLBuilder; overload;
            function orWhere(field, exp, value : string) : TSQLBuilder; overload;
            function orWhere(field, value : string) : TSQLBuilder; overload;
            function where(field, exp : string; value : Currency) : TSQLBuilder; overload;
            function where(field :string; value : Currency) : TSQLBuilder; overload;
            function where(field : string; value : boolean) : TSQLBuilder; overload;
            function where(expression: TSQLExpression) : TSQLBuilder; overload;
            function join(table, ref1, exp, ref2: string) : TSQLBuilder; overload;
            function join(table, ref1, ref2: string) : TSQLBuilder; overload;
            function join(table, condition: string) : TSQLBuilder; overload;
            function whereIn(field: string; list: TList<Currency>) : TSQLBuilder;
            function whereNotIn(field: string; list: TList<Currency>) : TSQLBuilder;
            function first(rows : integer) : TSQLBuilder;
            function skip(rows : integer) : TSQLBuilder;
            function whereNull(field : string) : TSQLBuilder;
            function orderBy(field, order : string) : TSQLBuilder;
            function count : Integer;
            procedure insert;
            procedure update;
            procedure delete;
            procedure executeProcedure;
            function get : TClientDataSet;
            function raw(sql: string): TClientDataSet;
            procedure rawExec(sql: string);
    end;



implementation

{ TSQLBuilder }

procedure TSQLBuilder.bindParam(field, value : string);
begin
    if not Assigned(self.FInsertFields) then
        self.FInsertFields := TStringList.Create;

    if not Assigned(self.FInsertValues) then
        self.FInsertValues := TStringList.Create;

    self.FInsertFields.Add(field);
    self.FInsertValues.Add(value);
end;

function TSQLBuilder.buildDelete: string;
var
    sql : string;
begin
    try
        sql := 'DELETE FROM ' + self.FFrom;

        if Assigned(self.FSQLExpression) then
        begin
            sql := sql + ' WHERE ' + self.FSQLExpression.build();
        end;

        Result := sql;
    finally
        self.clear;
    end;
end;

function TSQLBuilder.buildExecuteProcedure: string;
var
    fields,
    values : string;
    i : integer;
begin
    try
        if Assigned(self.FInsertFields) then
        begin
            for i := 0 to self.FInsertFields.Count-1 do
            begin
                values := values + self.FInsertValues[i];

                if i < self.FInsertFields.Count-1 then
                begin
                    fields := fields + ', ';
                    values := values + ', ';
                end;
            end;
        end;

        Result:= format('EXECUTE PROCEDURE %s(%s)', [
            self.FFrom,
            values
        ]);
    finally
        self.clear;
    end;
end;

function TSQLBuilder.buildInsert: string;
var
    fields,
    values : string;
    i : integer;
begin
    try
        if Assigned(self.FInsertFields) then
        begin
            for i := 0 to self.FInsertFields.Count-1 do
            begin
                fields := fields + self.FInsertFields[i];
                values := values + self.FInsertValues[i];

                if i < self.FInsertFields.Count-1 then
                begin
                    fields := fields + ', ';
                    values := values + ', ';
                end;
            end;
        end;

        Result:= format('INSERT INTO %s(%s) VALUES(%s)', [
            self.FFrom,
            fields,
            values
        ]);
    finally
        self.clear;
    end;
end;

function TSQLBuilder.buildSelect(): String;
var
    sql : string;
    i : integer;
begin
    sql := 'SELECT ';

    try
        if self.FFirst > 0 then
            sql := sql + 'FIRST ' + IntToStr(self.FFirst) + ' ';

        if self.FSkip > 0 then
            sql := sql + 'SKIP ' + IntToStr(self.FSkip) + ' ';

        if not Assigned(self.FSelectFields) then
        begin
             self.select('*');
        end;

        for I := 0 to self.FSelectFields.Count-1 do
        begin
            sql := sql + self.FSelectFields[i];

            if i < self.FSelectFields.Count-1 then
                sql := sql + ', ';
        end;

        sql := sql + ' FROM ' + self.FFrom;

        if Assigned(self.FJoins) then
        begin
            for i := 0 to self.FJoins.Count - 1 do
            begin
                sql := sql + ' JOIN ' + self.FJoins[i];
            end;
        end;

        if Assigned(self.FSQLExpression) then
        begin
            sql := sql + ' WHERE ' + self.FSQLExpression.build();
        end;

        if Assigned(self.FOrder) then
        begin
            sql := sql + ' ORDER BY ';

            for I := 0 to self.FOrder.Count-1 do
            begin
                sql := sql + self.FOrder[i];

                if i < self.FOrder.Count-1 then
                    sql := sql + ', ';
            end;
        end;
        
        result:= sql;
    finally
        self.clear;
    end;
end;

function TSQLBuilder.buildUpdate: string;
var
    sql,
    fields,
    values : string;
    i : integer;
begin
    try
        sql := 'UPDATE ' + self.FFrom + ' SET ';

        if Assigned(self.FInsertFields) then
        begin
            for i := 0 to self.FInsertFields.Count-1 do
            begin
                sql := sql + self.FInsertFields[i] + ' = ' + self.FInsertValues[i];

                if i < self.FInsertFields.Count-1 then
                begin
                    sql := sql + ', ';
                end;
            end;
        end;

        if Assigned(self.FSQLExpression) then
        begin
            sql := sql + ' WHERE ' + self.FSQLExpression.build();
        end;

        Result := sql;
    finally
        self.clear;
    end;
end;

procedure TSQLBuilder.clear;
begin
    if Assigned(self.FSelectFields) then
        FreeAndNil(self.FSelectFields);

    if Assigned(Self.FInsertFields) then
        FreeAndNil(Self.FInsertFields);

    if Assigned(self.FInsertValues) then
        FreeAndNil(self.FInsertValues);

    if Assigned(self.FSQLExpression) then
        FreeAndNil(self.FSQLExpression);

    if Assigned(self.FOrder) then
        FreeAndNil(self.FOrder);

    if Assigned(self.FJoins) then
        FreeAndNil(self.FJoins);

    FFirst := 0;
    FSkip := 0;
    FFrom := '';
end;

function TSQLBuilder.count: Integer;
var
    dataset : TClientDataSet;
begin
    dataset := TClientDataSet.Create(nil);

    try
        self.select('COUNT(*)');
        dataset.Data := self.FDBConn.fetchAssoc(self.buildSelect);
        dataset.First;
        Result:= dataset.FieldByName('COUNT').AsInteger;
    finally
        FreeAndNil(dataset);
    end;
end;

constructor TSQLBuilder.Create(dbUser, dbPassword, dbHost, dbName: string);
begin
    self.FDBConn := TDBConnection.Create(dbUser, dbPassword, dbHost, dbName);
end;

procedure TSQLBuilder.delete;
begin
    self.FDBConn.exec(self.buildDelete);
end;

destructor TSQLBuilder.destroy;
begin
    if Assigned(self.FSelectFields) then
        FreeAndNil(self.FSelectFields);

    if Assigned(self.FInsertFields) then
        FreeAndNil(self.FInsertFields);

    if Assigned(self.FInsertValues) then
        FreeAndNil(self.FInsertValues);

    if Assigned(self.FOrder) then
        FreeAndNil(self.FOrder);

    if Assigned(self.FSQLExpression) then
        FreeAndNil(self.FSQLExpression);

    if Assigned(self.FDBConn) then
        FreeAndNil(self.FDBConn);

    inherited;
end;

procedure TSQLBuilder.executeProcedure;
begin
    self.FDBConn.exec(self.buildExecuteProcedure);
end;

function TSQLBuilder.fill(field, value: string): TSQLBuilder;
begin
    self.bindParam(field, Format('''%s''', [trim(StringReplace(value, '''', '', [rfReplaceAll]))]));
    result:= self;
end;

function TSQLBuilder.fill(field: string; value: currency): TSQLBuilder;
begin
    self.bindParam(field, StringReplace(CurrToStr(value),',', '.', []));
    result:= self;
end;

function TSQLBuilder.fill(field: string; value: boolean): TSQLBuilder;
begin
    if value then
        self.bindParam(field, '1')
    else
        self.bindParam(field, '0');

    result:= self;
end;

function TSQLBuilder.first(rows: integer) : TSQLBuilder;
begin
    self.FFirst := 12;
    self.FFirst := rows;

    result:=self;
end;

function TSQLBuilder.from(table: string): TSQLBuilder;
begin
    self.FFrom := table;
    result:= self;
end;

function TSQLBuilder.genId(generator: string; step: integer): TSQLBuilder;
begin
    self.bindParam('ID', Format('gen_id(%s, %d)', [trim(generator), step]));
    result:= self;
end;

function TSQLBuilder.get: TClientDataSet;
var
    dataset : TClientDataSet;
begin
    dataset := TClientDataSet.Create(nil);
    dataset.Data := self.FDBConn.fetchAssoc(self.buildSelect);
    dataset.First;
    Result:= dataset;
end;

procedure TSQLBuilder.insert;
begin
    self.FDBConn.exec(self.buildInsert);
end;

function TSQLBuilder.join(table, ref1, exp, ref2: string): TSQLBuilder;
begin
    Result := self.join(table, format('%s %s %s', [ref1, exp, ref2]));
end;

function TSQLBuilder.join(table, ref1, ref2: string): TSQLBuilder;
begin
    Result := self.join(table, format('%s = %s', [ref1, ref2]));
end;

function TSQLBuilder.join(table, condition: string): TSQLBuilder;
begin
    Result := self;

    if not Assigned(self.FJoins) then
        self.FJoins := TStringList.Create;

    self.FJoins.Add(Format('%s ON %s', [table, condition]));
end;

function TSQLBuilder.orderBy(field, order: string): TSQLBuilder;
begin
    if not Assigned(self.FOrder) then
        self.FOrder := TStringList.Create;

   self.FOrder.Add(format('%s %s', [field, order]));
   result:= self;
end;

function TSQLBuilder.orWhere(field, value: string): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        FSQLExpression := TSQLExpression.Create;

    FSQLExpression.orWhere(field, value);

    Result:= self; 
end;

function TSQLBuilder.raw(sql: string): TClientDataSet;
var
    dataset : TClientDataSet;
begin
    dataset := TClientDataSet.Create(nil);
    dataset.Data := self.FDBConn.fetchAssoc(sql);
    dataset.First;
    Result:= dataset;
end;

procedure TSQLBuilder.rawExec(sql: string);
begin
    self.FDBConn.exec(sql);
end;

function TSQLBuilder.orWhere(field, exp, value: string): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        FSQLExpression := TSQLExpression.Create;

    FSQLExpression.orWhere(field, value);

    Result:= self;    
end;

function TSQLBuilder.select(field : string): TSQLBuilder;
begin
    if not Assigned(self.FSelectFields) then
        self.FSelectFields := TStringList.Create;

    self.FSelectFields.Add(field);
    result:=self;
end;

function TSQLBuilder.skip(rows: integer): TSQLBuilder;
begin
    self.FSkip := rows;
    result:=self;
end;

procedure TSQLBuilder.update;
begin
    self.FDBConn.exec(self.buildUpdate);
end;

function TSQLBuilder.where(field, exp: string; value: Currency): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.where(field, exp, value);

    Result:= self;
end;

function TSQLBuilder.where(field, value: string): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.where(field, value);

    Result:= self;
end;

function TSQLBuilder.where(field, exp, value: string): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.where(field, exp, value);

    Result:= self;
end;


function TSQLBuilder.where(field: string; value: boolean): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    if value then
        self.FSQLExpression.where(field, 1)
    else
        self.FSQLExpression.where(field, 0);

    Result:= self;
end;

function TSQLBuilder.whereIn(field: string; list: TList<Currency>): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.whereIn(field, list);
    Result:= self;
end;

function TSQLBuilder.whereNotIn(field: string;
  list: TList<Currency>): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.whereNotIn(field, list);
    Result:= self;
end;

function TSQLBuilder.whereNull(field: string): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.whereNull(field);

    Result:= self;
end;

function TSQLBuilder.where(field: string; value: Currency): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.where(field, value);

    Result:= self;
end;

function TSQLBuilder.where(expression: TSQLExpression): TSQLBuilder;
begin
    if not Assigned(self.FSQLExpression) then
        self.FSQLExpression := TSQLExpression.Create;

    self.FSQLExpression.where(expression);

    Result:= self;
end;

end.
