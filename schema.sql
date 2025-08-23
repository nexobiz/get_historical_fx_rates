
-- schema.sql
-- Run this in Supabase SQL editor (or psql) to create the exchange_rates table.

create table if not exists public.exchange_rates (
  rate_date date not null,
  base_currency text not null default 'USD',
  symbol text not null,
  rate numeric not null,
  provider text not null default 'exchangerate.host',
  fetched_at timestamptz not null default now(),
  constraint exchange_rates_pkey primary key (rate_date, base_currency, symbol)
);

-- Helpful index for queries like: select * where symbol='CAD' and rate_date between ...
create index if not exists exchange_rates_symbol_date_idx
  on public.exchange_rates (symbol, rate_date);

-- Recommended RLS posture: keep enabled, and use the Service Role key from your secure server to bypass RLS.
alter table public.exchange_rates enable row level security;

-- Example policy for read-only access to authenticated users (optional, adapt to your app):
do $$ begin
  if not exists (select 1 from pg_policies where policyname = 'Allow read to authenticated users' and tablename = 'exchange_rates') then
    create policy "Allow read to authenticated users"
      on public.exchange_rates for select
      to authenticated
      using (true);
  end if;
end $$;
