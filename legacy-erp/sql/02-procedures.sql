USE ERPDB;
GO

-- The only write path the legacy application uses. It records the movement and
-- updates stock in one transaction. Like many legacy systems, it does not stop
-- stock from going negative.
CREATE OR ALTER PROCEDURE dbo.SP_INV_MOVIMIENTO
    @ART_COD CHAR(12),
    @BOD_COD CHAR(4),
    @MOV_TIP CHAR(2),
    @MOV_CAN DECIMAL(12,2),
    @MOV_USR VARCHAR(10),
    @MOV_REF VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @SIGNO INT = CASE @MOV_TIP
        WHEN 'VT' THEN -1
        WHEN 'AN' THEN -1
        WHEN 'DV' THEN 1
        WHEN 'RC' THEN 1
        WHEN 'AP' THEN 1
    END;

    IF @SIGNO IS NULL
        THROW 50001, 'MOV_TIP invalido', 1;

    BEGIN TRANSACTION;

    INSERT INTO dbo.INV_MOV (ART_COD, BOD_COD, MOV_TIP, MOV_CAN, MOV_USR, MOV_REF)
    VALUES (@ART_COD, @BOD_COD, @MOV_TIP, @MOV_CAN, @MOV_USR, @MOV_REF);

    UPDATE dbo.INV_STK
       SET STK_EXI = STK_EXI + (@SIGNO * @MOV_CAN),
           FEC_ULT = GETDATE()
     WHERE ART_COD = @ART_COD
       AND BOD_COD = @BOD_COD;

    IF @@ROWCOUNT = 0
        INSERT INTO dbo.INV_STK (ART_COD, BOD_COD, STK_EXI)
        VALUES (@ART_COD, @BOD_COD, @SIGNO * @MOV_CAN);

    COMMIT TRANSACTION;
END;
GO
