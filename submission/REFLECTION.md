# Reflection

Anti-pattern ma team toi de gap nhat la **bien Bronze thanh "swamp"**: do du lieu vao vo to chuc, schema troi noi, va sua truc tiep len cung mot bang cho moi nhu cau ad-hoc. Ly do la khi deadline gap, team thuong uu tien "chay duoc" hon governance, dan den viec bo qua data contract, bo qua schema enforcement, va khong tach ro Bronze/Silver/Gold.

Rui ro thuc te la chat luong du lieu giam theo thoi gian: dashboard cung KPI nhung moi nhom lay mot dinh nghia khac nhau, truy vet su co cham vi khong ro lineage, va rollback kho khi co bad write. Voi khoi luong lon, chi phi compute/storage cung tang vi scan lai du lieu ban va file nho manh mun.

Huong giam thieu la giu Bronze immutable, ap schema checks ngay tu ingest, chuan hoa o Silver, va chi cho analytics tong hop o Gold. Team cung can bat buoc logging lineage va dung time travel cho cac tinh huong rollback.
