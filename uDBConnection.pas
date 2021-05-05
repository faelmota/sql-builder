unit uDBConnection;

interface

uses
  System.SysUtils, System.Classes,
  Data.DBXFirebird, Datasnap.Provider, Datasnap.DBClient, Data.DB,
  Data.SqlExpr, System.IniFiles, midas, midaslib;

type
    TDBConnection = class
      private
          { Private declarations }
          FConn: TSQLConnection;
          FDBUser: string;
          FDBPassword: string;
          FDBHost: string;
          FDBName: string;
          function getConn: TSQLConnection;
      public
          { Public declarations }
          constructor Create(dbUser, dbPassword, dbHost, dbName: string);
          destructor Destroy; override;
          function fetchAssoc(sql : String) : OleVariant;
          procedure exec( sql: String );
  end;

implementation

{ TDBConnection }
 
function TDBConnection.getConn: TSQLConnection;
begin
    if not Assigned(self.FConn) then
    begin
        self.FConn := TSQLConnection.Create(nil);

        with self.FConn do
        begin
            Connected := false;
            ConnectionName := 'FBConnection';
            DriverName := 'Firebird';
            GetDriverFunc := 'getSQLDriverINTERBASE';
            KeepConnection := false;
            LibraryName := 'dbxfb.dll';
            LoadParamsOnConnect := false;
            LoginPrompt := false;
            KeepConnection := true;
            VendorLib := 'dbxfb.dll';
            Params.Clear;

            Params.Values['Password']:= self.FDBPassword;
            Params.Values['User_Name']:= self.FDBUser;
            Params.Values['Database']:= format('%s:%s', [self.FDBHost, self.FDBName]);
            Params.Values['AutoCommit']:= 'true';
            Params.Values['Connection Timeout']:= '-1';
            Params.Values['CharSet']:= 'UTF8';
            Connected := True;
        end;
    end;

    Result:= self.FConn;
end;

constructor TDBConnection.Create(dbUser, dbPassword, dbHost, dbName: string);
begin
    inherited Create;
    self.FDBUser := dbUser;
    self.FDBPassword := dbPassword;
    self.FDBHost := dbHost;
    self.FDBName := dbName;
end;

destructor TDBConnection.Destroy;
begin
    if Assigned(self.FConn) then
        FreeAndNil(self.FConn);

    inherited;
end;

procedure TDBConnection.exec(sql: String);
var
  SQLQuery : TSQLQuery;
begin
    SQLQuery := TSQLQuery.Create(nil);

    try
        SQLQuery.SQLConnection := getConn;
        SQLQuery.Close;
        SQLQuery.CommandText:= sql;
        SQLQuery.ExecSQL;
    finally
        FreeAndNil(SQLQuery);
    end;
end;

function TDBConnection.fetchAssoc(sql: String): OleVariant;
var
  SQLQuery : TSQLQuery;
  DataSetProvider : TDataSetProvider;
begin
    SQLQuery := TSQLQuery.Create(nil);
    DataSetProvider := TDataSetProvider.Create(nil);

    try
        DataSetProvider.Options := [poAllowCommandText,poUseQuoteChar];
        DataSetProvider.DataSet := SQLQuery;
        SQLQuery.SQLConnection := getConn;
        DataSetProvider.DataSet := SQLQuery;
        SQLQuery.Close;
        SQLQuery.CommandText:=sql;
        Result := DataSetProvider.Data;
    finally
        FreeAndNil(SQLQuery);
        FreeAndNil(DataSetProvider);
    end;
end;

end.
