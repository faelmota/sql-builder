unit uSQLExpression;

interface

uses
    classes, System.SysUtils, System.Generics.Collections, uHelper;

type
    TSQLExpression = class
        private
            FWhere : TStringList;
            FConnectives : TStringList;
            procedure AddExpression(connective, expression : String);
        public
            destructor Destroy; override;
            function where(field, exp, value : string) : TSQLExpression; overload;
            function where(field, value : string) : TSQLExpression; overload;
            function where(field : string; value : currency) : TSQLExpression; overload;
            function orWhere(field, exp, value : string) : TSQLExpression; overload;
            function orWhere(field, value : string) : TSQLExpression; overload;
            function where(field, exp : string; value : Currency) : TSQLExpression; overload;
            function orWhere(field, exp : string; value : Currency) : TSQLExpression; overload;
            function whereNull(field : string) : TSQLExpression;
            function whereIn(field: string; list: TList<Currency>) : TSQLExpression;
            function whereNotIn(field: string; list: TList<Currency>) : TSQLExpression;
            function where(sqlExpression: TSQLExpression) : TSQLExpression; overload;
            function build : string;
    end;

implementation

{ TSQLExpression }

procedure TSQLExpression.AddExpression(connective, expression : String);
begin
    if not Assigned(self.FConnectives) then
        self.FConnectives := TStringList.Create;

    if not Assigned(self.FWhere) then
        self.FWhere := TStringList.Create;

    self.FWhere.Add(expression);
    self.FConnectives.Add(connective);   
end;

function TSQLExpression.build: string;
var 
    exp : String;
    i : integer;
begin
    try
        exp := '';

        for i := 0 to self.FWhere.Count-1 do
        begin
            exp := exp + self.FWhere[i];

            if i < self.FWhere.Count-1 then
                exp := exp + ' ' + self.FConnectives[i+1];

            if i < self.FWhere.Count-1 then
                exp := exp + ' ';
        end;

        Result := exp;
    finally
        FreeAndNil(self.FWhere);
        FreeAndNil(self.FConnectives);
    end;
end;

destructor TSQLExpression.Destroy;
begin
    if Assigned(self.FWhere) then
        FreeAndNil(self.FWhere);

    if Assigned(self.FConnectives) then
        FreeAndNil(FConnectives);

    inherited;
end;

function TSQLExpression.orWhere(field, exp, value : string): TSQLExpression;
begin
    self.AddExpression('OR', Format('%s %s ''%s''', [field, exp, value]));    
    Result:= self;
end;

function TSQLExpression.where(field, exp, value: string): TSQLExpression;
begin
    self.AddExpression('AND', Format('%s %s ''%s''', [field, exp, value]));
    Result:= self;
end;

function TSQLExpression.where(field, value: string): TSQLExpression;
begin
    self.AddExpression('AND', Format('%s = ''%s''', [field, value]));
    Result:= self;
end;

function TSQLExpression.orWhere(field, exp: string; value: Currency): TSQLExpression;
begin
    self.AddExpression('OR', Format('%s %s %s', [field, exp, CurrToStr(value)]));
    Result:= self;
end;

function TSQLExpression.orWhere(field, value: string): TSQLExpression;
begin
    self.AddExpression('OR', Format('%s = %s', [field, value]));
    Result:= self;
end;

function TSQLExpression.where(field, exp: string; value: Currency): TSQLExpression;
begin
    self.AddExpression('AND', Format('%s %s %s', [field, exp, CurrToStr(value)]));
    Result:= self;
end;

function TSQLExpression.where(sqlExpression: TSQLExpression): TSQLExpression;
begin
    self.AddExpression('AND', Format('(%s)', [sqlExpression.build]));
    Result := self;
end;

function TSQLExpression.whereIn(field: string;
  list: TList<Currency>): TSQLExpression;
begin
    if list.Count>0 then
        self.AddExpression('AND', format('%s IN (%s)', [field, ListToString(list)]));

    Result := self;
end;

function TSQLExpression.where(field: string; value: currency): TSQLExpression;
begin
    self.AddExpression('AND', Format('%s = %s', [field, CurrToStr(value)]));
    Result:= self;
end;

function TSQLExpression.whereNotIn(field: string;
  list: TList<Currency>): TSQLExpression;
begin
    if list.Count>0 then
        self.AddExpression('AND', format('%s NOT IN (%s)', [field, ListToString(list)]));

    Result := self;
end;

function TSQLExpression.whereNull(field : string): TSQLExpression;
begin
    self.AddExpression('AND', Format('%s IS NULL', [field]));
    Result:= self;
end;

end.
