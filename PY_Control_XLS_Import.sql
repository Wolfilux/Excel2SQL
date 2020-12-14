CREATE TABLE [dbo].[PY_Control_XLS_Import](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[SourcePath] [nvarchar](2000) NULL,
	[SourceFile] [nvarchar](2000) NULL,
	[DestinationTable] [nvarchar](255) NULL,
	[Sheets] [nvarchar](255) NULL,
	[RowsToSkip] [int] NULL,
	[HeaderRow] [int] NULL,
	[TruncateOnLoad] [bit] NULL
) ON [PRIMARY]
